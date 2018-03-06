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

#include "repo-storage.hpp"
#include "config.hpp"

#include <istream>

#include <ndn-cxx/util/logger.hpp>

namespace repo {

NDN_LOG_INIT(repo.RepoStorage);

RepoStorage::RepoStorage(Storage& store)
  : m_storage(store)
{
}

bool
RepoStorage::insertData(const Data& data)
{
  bool isExist = m_storage.has(data.getFullName());

  if (isExist)
  {
    NDN_LOG_DEBUG("Data already in database, regarded as successful data insertion.");
    return true;
  }
  else
  {
    int64_t id = m_storage.insert(data);
    NDN_LOG_DEBUG("Insert ID: " << id << ", full name:" << data.getFullName());
    if (id == -1)
      return false;

    afterDataInsertion(data.getName()); // not sure, fullname?
    return true;
  }
}

ssize_t
RepoStorage::deleteData(const Name& name)
{
  NDN_LOG_DEBUG("Delete: " << name);
  bool hasError = false;
  // this cause the unit test failed if not give find bool true
  std::pair<int64_t, Name> idName = m_storage.find(name).first;
  NDN_LOG_DEBUG("idName: " << idName.first << " " << idName.second);

  int64_t count = 0;
  while (idName.first != 0) {
    bool resultDb = m_storage.erase(idName.second);
    if (resultDb) {
      afterDataDeletion(idName.second.getSubName(0, -2)); // the exact name, right?
      count++;
    }
    else {
      hasError = true;
    }
    NDN_LOG_DEBUG("Delete: " << name << ", found " << idName.second << ", count " << count << ", result " << resultDb);
    idName = m_storage.find(name).first;
  }
  if (hasError)
    return -1;
  else
    return count;
}

ssize_t
RepoStorage::deleteData(const Interest& interest)
{
  return deleteData(interest.getName());
}

shared_ptr<Data>
RepoStorage::readData(const Interest& interest) const
{
  NDN_LOG_DEBUG("Reading data for " << interest.getName());

  return m_storage.read(interest.getName());
}


} // namespace repo
