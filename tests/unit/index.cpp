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

#include "storage/index.hpp"

#include "../sqlite-fixture.hpp"
#include "../dataset-fixtures.hpp"

#include <iostream>

#include <ndn-cxx/security/signing-helpers.hpp>
#include <ndn-cxx/util/sha256.hpp>
#include <ndn-cxx/util/random.hpp>

#include <boost/mpl/push_back.hpp>
#include <boost/test/unit_test.hpp>

namespace repo {
namespace tests {

BOOST_AUTO_TEST_SUITE(Index)

class FindFixture
{
protected:
  FindFixture()
    : m_index(std::numeric_limits<size_t>::max())
  {
  }

  Name
  insert(int id, const Name& name)
  {
    shared_ptr<Data> data = make_shared<Data>(name);
    data->setContent(reinterpret_cast<const uint8_t*>(&id), sizeof(id));
    m_keyChain.sign(*data, ndn::signingWithSha256());
    data->wireEncode();
    m_index.insert(*data, id);

    return data->getFullName();
  }

  Interest&
  startInterest(const Name& name)
  {
    m_interest = make_shared<Interest>(name);
    return *m_interest;
  }

  int
  find()
  {
    std::pair<int, Name> found = m_index.find(*m_interest);
    return found.first;
  }

protected:
  repo::Index m_index;
  KeyChain m_keyChain;
  shared_ptr<Interest> m_interest;
};

BOOST_FIXTURE_TEST_SUITE(Find, FindFixture)

BOOST_AUTO_TEST_CASE(EmptyDataName)
{
  insert(1, "ndn:/");
  startInterest("ndn:/");
  BOOST_CHECK_EQUAL(find(), 1);
}

BOOST_AUTO_TEST_CASE(EmptyInterestName)
{
  insert(1, "ndn:/A");
  startInterest("ndn:/");
  BOOST_CHECK_EQUAL(find(), 1);
}

BOOST_AUTO_TEST_CASE(ExactName)
{
  insert(1, "ndn:/");
  insert(2, "ndn:/A");
  insert(3, "ndn:/A/B");
  insert(4, "ndn:/A/C");
  insert(5, "ndn:/D");

  startInterest("ndn:/A");
  BOOST_CHECK_EQUAL(find(), 2);
}

BOOST_AUTO_TEST_CASE(FullName)
{
  Name n1 = insert(1, "ndn:/A");
  Name n2 = insert(2, "ndn:/A");

  startInterest(n1);
  BOOST_CHECK_EQUAL(find(), 1);

  startInterest(n2);
  BOOST_CHECK_EQUAL(find(), 2);
}


BOOST_AUTO_TEST_SUITE_END() // Find


template<class Dataset>
class Fixture : public Dataset
{
public:
  Fixture()
    : index(65535)
  {
  }

public:
  std::map<int64_t, shared_ptr<Data> > idToDataMap;
  repo::Index index;
};

BOOST_FIXTURE_TEST_CASE_TEMPLATE(Bulk, T, CommonDatasets, Fixture<T>)
{
  BOOST_TEST_MESSAGE(T::getName());

  for (typename T::DataContainer::iterator i = this->data.begin();
       i != this->data.end(); ++i)
    {
      int64_t id = std::abs(static_cast<int64_t>(ndn::random::generateWord64()));
      this->idToDataMap.insert(std::make_pair(id, *i));

      BOOST_CHECK_EQUAL(this->index.insert(**i, id), true);
    }

  BOOST_CHECK_EQUAL(this->index.size(), this->data.size());

  for (typename T::InterestContainer::iterator i = this->interests.begin();
       i != this->interests.end(); ++i)
    {
      std::pair<int64_t, Name> item = this->index.find(i->first);

      BOOST_REQUIRE_GT(item.first, 0);
      BOOST_REQUIRE(this->idToDataMap.count(item.first) > 0);

      BOOST_TEST_MESSAGE(i->first);
      BOOST_CHECK_EQUAL(*this->idToDataMap[item.first], *i->second);

      BOOST_CHECK_EQUAL(this->index.hasData(*i->second), true);
    }
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace tests
} // namespace repo
