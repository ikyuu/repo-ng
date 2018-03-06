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

#include "storage.hpp"

#include <ndn-cxx/util/sha256.hpp>
#include <ndn-cxx/security/signature-sha256-with-rsa.hpp>
#include <ndn-cxx/util/string-helper.hpp>

namespace repo {

std::string
Storage::toByteaHex(const uint8_t* s, size_t count)
{
  return "E'\\\\x" + ndn::toHex(s, count) + "'";
}

std::string
Storage::toByteaHex(const ndn::Block& block, bool wantValueOnly)
{
  if (wantValueOnly) {
    return toByteaHex(block.value(), block.value_size());
  }
  else {
    return toByteaHex(block.wire(), block.size());
  }
}

} // namespace repo