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

#include "storage.hpp"

#include <ndn-cxx/util/sha256.hpp>
#include <ndn-cxx/security/signature-sha256-with-rsa.hpp>

namespace repo {

const ndn::ConstBufferPtr
Storage::computeKeyLocatorHash(const Data& data)
{
  const ndn::Signature& signature = data.getSignature();
  if (signature.hasKeyLocator()) {
    const Block& block = signature.getKeyLocator().wireEncode();
    return ndn::util::Sha256::computeDigest(block.wire(), block.size());
  }
  else {
    BOOST_THROW_EXCEPTION(Error("Data has no KeyLocator!"));
  }
  return NULL;
}

}