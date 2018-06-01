/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2017, Regents of the University of California.
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

#include "handles/sync-handle.hpp"
#include "storage/sqlite-storage.hpp"

#include "command-fixture.hpp"
#include "../repo-storage-fixture.hpp"
#include "../dataset-fixtures.hpp"

#include <ndn-cxx/util/random.hpp>
#include <ndn-cxx/util/time.hpp>

#include <boost/mpl/vector.hpp>
#include <boost/test/unit_test.hpp>

namespace repo {
namespace tests {

using ndn::time::milliseconds;
using ndn::time::seconds;
using ndn::EventId;

// All the test cases in this test suite should be run at once.
BOOST_AUTO_TEST_SUITE(TestBasicCommandSyncDelete)

const static uint8_t content[8] = {3, 1, 4, 1, 5, 9, 2, 6};

template<class Dataset>
class Fixture : public CommandFixture, public RepoStorageFixture, public Dataset
{
public:
  Fixture()
    : syncHandle(repoFace, *handle, keyChain, scheduler, validator)
    , syncFace(repoFace.getIoService())
    , syncPrefix("/sync")
    , producerPrefix("/producer")
    , producerSyncSocket(syncPrefix,
                         producerPrefix,
                         ref(syncFace),
                         bind(&Fixture::processSyncUpdate, this, _1)
                         )
  {
    Name cmdPrefix("/repo/command");

    producerSyncSocket.addSyncNode("/repo");
    producerSyncSocket.addSyncNode(producerPrefix);

    repoFace.registerPrefix(cmdPrefix, nullptr,
      [] (const Name& cmdPrefix, const std::string& reason) {
        BOOST_FAIL("Command prefix registration error: " << reason);
      });
    syncFace.registerPrefix(syncPrefix, nullptr,
      [] (const Name& syncPrefix, const std::string& reason) {
        BOOST_FAIL("Command prefix registration error: " << reason);
      });
    producerFace.registerPrefix(producerPrefix, nullptr,
      [] (const Name& producerPrefix, const std::string& reason) {
        BOOST_FAIL("Command prefix registration error: " << reason);
      });
    syncHandle.listen(cmdPrefix);
  }

  static void
  terminate(boost::asio::io_service& ioService,
            const boost::system::error_code& error,
            int signalNo,
            boost::asio::signal_set& signalSet);

  void
  scheduleSyncEvent();

  void
  onRegisterFailed(const std::string& reason);

  void
  delayedInterest();

  void
  onSyncStartData(const Interest& interest, const Data& data);

  void
  onSyncStopData(const Interest& interest, const Data& data);

  void
  onSyncStartTimeout(const Interest& interest);

  void
  sendSyncStartInterest(const Interest& interest);

  void
  sendSyncStopInterest(const Interest& interest);

  void
  onSyncInterest(const Name& prefix, const Interest& interest);

  void
  checkSyncOk(const Interest& interest);

  void
  processSyncUpdate(const std::vector<chronosync::MissingDataInfo>& updates) {}

public:
  SyncHandle syncHandle;
  ndn::Face syncFace;
  ndn::Face producerFace;
  boost::asio::io_service producerIoService;
  std::map<Name, EventId> syncEvents;
  ndn::time::milliseconds m_lastUsedTimestamp = 0_ms;

  Name syncPrefix;
  Name producerPrefix;
  chronosync::Socket producerSyncSocket;
};

template<class T> void
Fixture<T>::terminate(boost::asio::io_service& ioService,
          const boost::system::error_code& error,
          int signalNo,
          boost::asio::signal_set& signalSet)
{
  if (error)
    return;

  if (signalNo == SIGINT ||
      signalNo == SIGTERM)
    {
      ioService.stop();
      std::cout << "Caught signal '" << strsignal(signalNo) << "', exiting..." << std::endl;
    }
  else
    {
      /// \todo May be try to reload config file
      signalSet.async_wait(std::bind(&Fixture<T>::terminate, std::ref(ioService),
                                     std::placeholders::_1, std::placeholders::_2,
                                     std::ref(signalSet)));
    }
}

template<class T> void
Fixture<T>::onRegisterFailed(const std::string& reason)
{
  BOOST_ERROR("ERROR: Failed to register prefix in local hub's daemon" + reason);
}

template<class T> void
Fixture<T>::delayedInterest()
{
  BOOST_ERROR("Fetching interest does not come. It may be satisfied in CS or something is wrong");
}

template<class T> void
Fixture<T>::onSyncInterest(const Name& prefix, const Interest& interest)
{
  std::cout<<"Got sync interest" << interest.getName() <<std::endl;
  // syncFace.put()
}

template<class T> void
Fixture<T>::onSyncStartData(const Interest& interest, const Data& data)
{
  RepoCommandResponse response;
  response.wireDecode(data.getContent().blockFromValue());

  int statusCode = response.getStatusCode();
  BOOST_CHECK_EQUAL(statusCode, 100);

  scheduler.scheduleEvent(milliseconds(100),
                          bind(&chronosync::Socket::publishData, &producerSyncSocket, content, sizeof(content), milliseconds(10000), 1, producerPrefix));

  // producerSyncSocket.publishData(content, sizeof(content), milliseconds(10000), 1, producerPrefix);

  scheduler.scheduleEvent(milliseconds(1000),
                          bind(&Fixture<T>::checkSyncOk, this, Interest(producerPrefix.appendSequenceNumber(1))));
}

template<class T> void
Fixture<T>::onSyncStopData(const Interest& interest, const Data& data)
{
  RepoCommandResponse response;
  response.wireDecode(data.getContent().blockFromValue());

  int statusCode = response.getStatusCode();
  BOOST_CHECK_EQUAL(statusCode, 101);
}

template<class T> void
Fixture<T>::onSyncStartTimeout(const Interest& interest)
{
  BOOST_ERROR("Sync command timeout");
}

template<class T> void
Fixture<T>::sendSyncStartInterest(const Interest& syncInterest)
{
  syncFace.expressInterest(syncInterest,
                            bind(&Fixture<T>::onSyncStartData, this, _1, _2),
                            bind(&Fixture<T>::onSyncStartTimeout, this, _1), // Nack
                            bind(&Fixture<T>::onSyncStartTimeout, this, _1));
}

template<class T> void
Fixture<T>::sendSyncStopInterest(const Interest& syncInterest)
{
  syncFace.expressInterest(syncInterest,
                            bind(&Fixture<T>::onSyncStopData, this, _1, _2),
                            bind(&Fixture<T>::onSyncTimeout, this, _1), // Nack
                            bind(&Fixture<T>::onSyncTimeout, this, _1));
}

template<class T> void
Fixture<T>::checkSyncOk(const Interest& interest)
{
  BOOST_TEST_MESSAGE(interest);
  Interest ttt("/producer");
  // shared_ptr<Data> data = handle->readData(interest);
  shared_ptr<Data> data = handle->readData(ttt);
  if (data) {
    int rc = memcmp(data->getContent().value(), content, sizeof(content));
    BOOST_CHECK_EQUAL(rc, 0);
  }
  else {
    std::cerr<<"Check Sync Failed"<<std::endl;
  }
}

template<class T> void
Fixture<T>::scheduleSyncEvent()
{
  Name syncCommandName("/repo/command/sync/start");
  RepoCommandParameter syncParameter;
  syncParameter.setName(syncPrefix);
  syncParameter.setInterestLifetime(milliseconds(50000));
  syncParameter.setSyncTimeout(milliseconds(1000000000));
  syncCommandName.append(syncParameter.wireEncode());

  ndn::time::milliseconds timestamp = ndn::time::toUnixTimestamp(ndn::time::system_clock::now());
    if (timestamp <= m_lastUsedTimestamp) {
      timestamp = m_lastUsedTimestamp + 1_ms;
    }
  m_lastUsedTimestamp = timestamp;
  syncCommandName.append(name::Component::fromNumber(timestamp.count()));
  syncCommandName.append(name::Component::fromNumber(ndn::random::generateWord64()));

  Interest syncStartInterest(syncCommandName);
  keyChain.sign(syncStartInterest);
  //schedule a job to express syncStartInterest
  scheduler.scheduleEvent(milliseconds(1000),
                          bind(&Fixture<T>::sendSyncStartInterest, this, syncStartInterest));

  syncFace.setInterestFilter(ndn::InterestFilter(producerPrefix).allowLoopback(false),
                             bind(&Fixture<T>::onSyncInterest, this, _1, _2));

  // Name syncStopName("/repo/command/sync/stop");
  // RepoCommandParameter syncStopParameter;
  // syncStopName.append(syncStopParameter.wireEncode());
  // Interest syncStopInterest(syncStopName);
  // keyChain.sign(syncStopInterest);

  // scheduler.scheduleEvent(milliseconds(10000),
  //                        bind(&Fixture<T>::sendSyncStopInterest, this, syncStopInterest));
}

typedef boost::mpl::vector< BasicDataset > Dataset;

BOOST_FIXTURE_TEST_CASE_TEMPLATE(SyncDelete, T, Dataset, Fixture<T>)
{
  // schedule events
  this->scheduler.scheduleEvent(seconds(0),
                                bind(&Fixture<T>::scheduleSyncEvent, this));

  this->repoFace.processEvents(seconds(500));
  // this->producerFace.processEvents(seconds(500));
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace tests
} // namespace repo