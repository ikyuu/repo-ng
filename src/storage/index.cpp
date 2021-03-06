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

#include "index.hpp"

#include <ndn-cxx/util/sha256.hpp>
#include <ndn-cxx/security/signature-sha256-with-rsa.hpp>

namespace repo {

/** @brief determines if entry can satisfy interest
 *  @param hash SHA256 hash of PublisherPublicKeyLocator if exists in interest, otherwise ignored
 */

Index::Index(size_t nMaxPackets)
  : m_maxPackets(nMaxPackets)
  , m_size(0)
{
}


bool
Index::insert(const Data& data, int64_t id)
{
  if (isFull())
    BOOST_THROW_EXCEPTION(Error("The Index is Full. Cannot Insert Any Data!"));
  Entry entry(data, id);
  bool isInserted = m_indexContainer.insert(entry).second;
  if (isInserted)
    ++m_size;
  return isInserted;
}

bool
Index::insert(const Name& fullName, int64_t id,
              const ndn::ConstBufferPtr& keyLocatorHash)
{
  if (isFull())
    BOOST_THROW_EXCEPTION(Error("The Index is Full. Cannot Insert Any Data!"));
  Entry entry(fullName, keyLocatorHash, id);
  bool isInserted = m_indexContainer.insert(entry).second;
  if (isInserted)
    ++m_size;
  return isInserted;
}


std::pair<int64_t,Name>
Index::find(const Interest& interest) const
{
  Name name = interest.getName();
  IndexContainer::const_iterator result = m_indexContainer.lower_bound(name);
  if (result != m_indexContainer.end())
    {
      // get the name of interest and match prefix
      return findFirstEntry(name, result);
    }
  else
    {
      return std::make_pair(0, Name());
    }
  
}

std::pair<int64_t,Name>
Index::find(const Name& name) const
{
  IndexContainer::const_iterator result = m_indexContainer.lower_bound(name);
  if (result != m_indexContainer.end())
    {
      return findFirstEntry(name, result);
    }
  else
    {
      return std::make_pair(0, Name());
    }
}

bool
Index::hasData(const Data& data) const
{
  Index::Entry entry(data, -1); // the id number is useless
  IndexContainer::const_iterator result = m_indexContainer.find(entry);
  return result != m_indexContainer.end();

}

std::pair<int64_t,Name>
Index::findFirstEntry(const Name& prefix,
                      IndexContainer::const_iterator startingPoint) const
{
  BOOST_ASSERT(startingPoint != m_indexContainer.end());
  if (prefix.isPrefixOf(startingPoint->getName()))
    {
      return std::make_pair(startingPoint->getId(), startingPoint->getName());
    }
  else
    {
      return std::make_pair(0, Name());
    }
}

bool
Index::erase(const Name& fullName)
{
  Entry entry(fullName);
  IndexContainer::const_iterator findIterator = m_indexContainer.find(entry);
  if (findIterator != m_indexContainer.end())
    {
      m_indexContainer.erase(findIterator);
      m_size--;
      return true;
    }
  else
    return false;
}

const ndn::ConstBufferPtr
Index::computeKeyLocatorHash(const KeyLocator& keyLocator)
{
  const Block& block = keyLocator.wireEncode();
  ndn::ConstBufferPtr keyLocatorHash = ndn::util::Sha256::computeDigest(block.wire(), block.size());
  return keyLocatorHash;
}


Index::Entry::Entry(const Data& data, int64_t id)
  : m_name(data.getFullName())
  , m_id(id)
{
  const ndn::Signature& signature = data.getSignature();
  if (signature.hasKeyLocator())
    m_keyLocatorHash = computeKeyLocatorHash(signature.getKeyLocator());
}

Index::Entry::Entry(const Name& fullName, const KeyLocator& keyLocator, int64_t id)
  : m_name(fullName)
  , m_keyLocatorHash(computeKeyLocatorHash(keyLocator))
  , m_id(id)
{
}

Index::Entry::Entry(const Name& fullName,
                    const ndn::ConstBufferPtr& keyLocatorHash, int64_t id)
  : m_name(fullName)
  , m_keyLocatorHash(keyLocatorHash)
  , m_id(id)
{
}

Index::Entry::Entry(const Name& name)
  : m_name(name)
{
}

} // namespace repo
