/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2018, Regents of the University of California.
 *
 * This file is part of NDN repo-ng (Next generation of NDN repository).
 * See AUTHORS.md for complete list of repo-ng authors and contributors.
 *
 * repo-ng is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * repo-ng is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * repo-ng, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "sync-handle.hpp"
#include "socket.hpp"
#include <ndn-cxx/util/logger.hpp>

namespace repo {

NDN_LOG_INIT(repo.SyncHandle);

static const milliseconds PROCESS_DELETE_TIME(10000);
static const milliseconds DEFAULT_INTEREST_LIFETIME(4000);

SyncHandle::SyncHandle(Face& face, RepoStorage& storageHandle, KeyChain& keyChain,
                         Scheduler& scheduler, Validator& validator)
  : BaseHandle(face, storageHandle, keyChain, scheduler)
  , m_validator(validator)
  , m_interestNum(0)
  , m_maxInterestNum(0)
  , m_interestLifetime(DEFAULT_INTEREST_LIFETIME)
  , m_syncTimeout(0)
  , m_startTime(steady_clock::now())
  , m_size(0)
{
}

void
SyncHandle::deleteProcess(const Name& name)
{
  m_processes.erase(name);
}

// Interest.
void
SyncHandle::onInterest(const Name& prefix, const Interest& interest)
{
  m_validator.validate(interest,
                       bind(&SyncHandle::onValidated, this, _1, prefix),
                       bind(&SyncHandle::onValidationFailed, this, _1, _2));
}

void
SyncHandle::onValidated(const Interest& interest, const Name& prefix)
{
  RepoCommandParameter parameter;
  try {
    extractParameter(interest, prefix, parameter);
  }
  catch (RepoCommandParameter::Error) {
    negativeReply(interest, 403);
    return;
  }

  processSyncCommand(interest, parameter);
}

void
SyncHandle::syncStop(const Name& name)
{
  m_processes[name].second = false;
  m_maxInterestNum = 0;
  m_interestNum = 0;
  m_startTime = steady_clock::now();
  m_syncTimeout = milliseconds(0);
  m_interestLifetime = DEFAULT_INTEREST_LIFETIME;
  m_size = 0;
  m_sock.reset();
}

void
SyncHandle::onValidationFailed(const Interest& interest, const ValidationError& error)
{
  std::cerr << error << std::endl;
  negativeReply(interest, 401);
}

void
SyncHandle::onData(const Interest& interest, const ndn::Data& data, const Name& name)
{
  m_validator.validate(data,
                       bind(&SyncHandle::onDataValidated, this, interest, _1, name),
                       bind(&SyncHandle::onDataValidationFailed, this, interest, _1, _2, name));
}

void
SyncHandle::onDataValidated(const Interest& interest, const Data& data, const Name& name)
{
  if (!m_processes[name].second){
    return;
  }
  if (getStorageHandle().insertData(data)){
    m_size++;
    if (!onRunning(name))
      return;
    Interest fetchInterest(interest.getName());
    fetchInterest.setInterestLifetime(m_interestLifetime);

    ++m_interestNum;
    getFace().expressInterest(fetchInterest,
                              bind(&SyncHandle::onData, this, _1, _2, name),
                              bind(&SyncHandle::onTimeout, this, _1, name), // Nack
                              bind(&SyncHandle::onTimeout, this, _1, name));
  }
  else {
    BOOST_THROW_EXCEPTION(Error("Insert into Repo Failed"));
  }
  m_processes[name].first.setInsertNum(m_size);
}

void
SyncHandle::onDataValidationFailed(const Interest& interest, const Data& data, const ValidationError& error, const Name& name)
{
  std::cerr << error << std::endl;
  if (!m_processes[name].second) {
    return;
  }
  if (!onRunning(name))
    return;

  Interest fetchInterest(interest.getName());
  fetchInterest.setInterestLifetime(m_interestLifetime);

  ++m_interestNum;
  getFace().expressInterest(fetchInterest,
                            bind(&SyncHandle::onData, this, _1, _2, name),
                            bind(&SyncHandle::onTimeout, this, _1, name), // Nack
                            bind(&SyncHandle::onTimeout, this, _1, name));
}

void
SyncHandle::onTimeout(const ndn::Interest& interest, const Name& name)
{
  std::cerr << "Timeout" << std::endl;
  if (!m_processes[name].second) {
    return;
  }
  if (!onRunning(name))
    return;

  Interest fetchInterest(interest.getName());
  fetchInterest.setInterestLifetime(m_interestLifetime);

  ++m_interestNum;
  getFace().expressInterest(fetchInterest,
                            bind(&SyncHandle::onData, this, _1, _2, name),
                            bind(&SyncHandle::onTimeout, this, _1, name), // Nack
                            bind(&SyncHandle::onTimeout, this, _1, name));

}

void
SyncHandle::listen(const Name& prefix)
{
  getFace().setInterestFilter(Name(prefix).append("sync").append("start"),
                              bind(&SyncHandle::onInterest, this, _1, _2));
  getFace().setInterestFilter(Name(prefix).append("sync").append("check"),
                              bind(&SyncHandle::onCheckInterest, this, _1, _2));
  getFace().setInterestFilter(Name(prefix).append("sync").append("stop"),
                              bind(&SyncHandle::onStopInterest, this, _1, _2));
}


void
SyncHandle::onStopInterest(const Name& prefix, const Interest& interest)
{
  m_validator.validate(interest,
                       bind(&SyncHandle::onStopValidated, this, _1, prefix),
                       bind(&SyncHandle::onStopValidationFailed, this, _1, _2));
}

void
SyncHandle::onStopValidated(const Interest& interest, const Name& prefix)
{
  RepoCommandParameter parameter;
  try {
    extractParameter(interest, prefix, parameter);
  }
  catch (RepoCommandParameter::Error) {
    negativeReply(interest, 403);
    return;
  }

  syncStop(parameter.getName());
  negativeReply(interest, 101);
}

void
SyncHandle::onStopValidationFailed(const Interest& interest, const ValidationError& error)
{
  std::cerr << error << std::endl;
  negativeReply(interest, 401);
}

void
SyncHandle::onCheckInterest(const Name& prefix, const Interest& interest)
{
  m_validator.validate(interest,
                       bind(&SyncHandle::onCheckValidated, this, _1, prefix),
                       bind(&SyncHandle::onCheckValidationFailed, this, _1, _2));
}

void
SyncHandle::onCheckValidated(const Interest& interest, const Name& prefix)
{
  RepoCommandParameter parameter;
  try {
    extractParameter(interest, prefix, parameter);
  }
  catch (RepoCommandParameter::Error) {
    negativeReply(interest, 403);
    return;
  }

  if (!parameter.hasName()) {
    negativeReply(interest, 403);
    return;
  }
  //check whether this process exists
  Name name = parameter.getName();
  if (m_processes.count(name) == 0) {
    std::cerr << "no such process name: " << name << std::endl;
    negativeReply(interest, 404);
    return;
  }

  RepoCommandResponse& response = m_processes[name].first;
    if (!m_processes[name].second) {
    response.setStatusCode(101);
  }

  reply(interest, response);

}

void
SyncHandle::onCheckValidationFailed(const Interest& interest, const ValidationError& error)
{
  std::cerr << error << std::endl;
  negativeReply(interest, 401);
}

void
SyncHandle::deferredDeleteProcess(const Name& name)
{
  getScheduler().scheduleEvent(PROCESS_DELETE_TIME,
                               bind(&SyncHandle::deleteProcess, this, name));
}

void
SyncHandle::processSyncCommand(const Interest& interest,
                                 RepoCommandParameter& parameter)
{
  // Sync start
  // if there is no syncTimeout specified, m_syncTimeout will be set as 0 and this handle will run forever
  if (parameter.hasSyncTimeout()) {
    m_syncTimeout = parameter.getSyncTimeout();
  }
  else {
    m_syncTimeout = milliseconds(0);
  }

  // if there is no maxInterestNum specified, m_maxInterestNum will be 0, which means infinity
  if (parameter.hasMaxInterestNum()) {
    m_maxInterestNum = parameter.getMaxInterestNum();
  }
  else {
    m_maxInterestNum = 0;
  }

  if (parameter.hasInterestLifetime()) {
    m_interestLifetime = parameter.getInterestLifetime();
  }

  reply(interest, RepoCommandResponse().setStatusCode(100));

  m_processes[parameter.getName()] =
                std::make_pair(RepoCommandResponse().setStatusCode(300), true);

  // create a new SyncSocket
  m_sock = make_shared<chronosync::Socket>(parameter.getName(),
                                           Name(),
                                           ref(getFace()),
                                           bind(&SyncHandle::processSyncUpdate, this, _1),
                                           Name()//SigningId
                                           );

}

void
SyncHandle::processSyncUpdate(const std::vector<chronosync::MissingDataInfo>& updates)
{
  NDN_LOG_DEBUG("<<< processing Tree Update");

  if (updates.empty()) {
    return;
  }

  std::vector<chronosync::NodeInfo> nodeInfos;

  for (size_t i = 0; i < updates.size(); i++) {
    // fetch missing data
    for (chronosync::SeqNo seq = updates[i].low; seq <= updates[i].high; ++seq) {
      Name interestName;
      interestName.append(updates[i].session).appendNumber(seq);

      Interest interest(interestName);
      interest.setInterestLifetime(m_interestLifetime);
      interest.setMustBeFresh(true);

      m_startTime = steady_clock::now();
      m_interestNum++;
      getFace().expressInterest(interest,
                            bind(&SyncHandle::onData, this, _1, _2, interestName),
                            bind(&SyncHandle::onTimeout, this, _1, interestName), // Nack
                            bind(&SyncHandle::onTimeout, this, _1, interestName));

      NDN_LOG_DEBUG("<<< Fetching " << updates[i].session << "/" << seq);
    }
  }
}


void
SyncHandle::negativeReply(const Interest& interest, int statusCode)
{
  RepoCommandResponse response;
  response.setStatusCode(statusCode);
  reply(interest, response);
}

bool
SyncHandle::onRunning(const Name& name)
{
  bool isTimeout = (m_syncTimeout != milliseconds::zero() &&
                    steady_clock::now() - m_startTime > m_syncTimeout);
  bool isMaxInterest = m_interestNum >= m_maxInterestNum && m_maxInterestNum != 0;
  if (isTimeout || isMaxInterest) {
    deferredDeleteProcess(name);
    syncStop(name);
    return false;
  }
  return true;
}

} // namespace repo
