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

#include "sqlite-storage.hpp"
#include "config.hpp"

#include <ndn-cxx/util/sha256.hpp>
#include <boost/filesystem.hpp>
#include <istream>

#include <ndn-cxx/util/logger.hpp>

namespace repo {

NDN_LOG_INIT(repo.SqliteStorage);

SqliteStorage::SqliteStorage(const string& dbPath)
{
  if (dbPath.empty()) {
    NDN_LOG_DEBUG("Create db file in local location [" << dbPath << "]. " );
    NDN_LOG_DEBUG("You can assign the path using -d option" );
    m_dbPath = string("ndn_repo.db");
  }
  else {
    boost::filesystem::path fsPath(dbPath);
    boost::filesystem::file_status fsPathStatus = boost::filesystem::status(fsPath);
    if (!boost::filesystem::is_directory(fsPathStatus)) {
      if (!boost::filesystem::create_directory(boost::filesystem::path(fsPath))) {
        BOOST_THROW_EXCEPTION(Error("Folder '" + dbPath + "' does not exists and cannot be created"));
      }
    }

    m_dbPath = dbPath + "/ndn_repo.db";
  }
  initializeRepo();
}


void
SqliteStorage::initializeRepo()
{
  char* errMsg = 0;

  int rc = sqlite3_open_v2(m_dbPath.c_str(), &m_db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
#ifdef DISABLE_SQLITE3_FS_LOCKING
                           "unix-dotfile"
#else
                           0
#endif
                           );

  if (rc == SQLITE_OK) {
    sqlite3_exec(m_db, "CREATE TABLE NDN_REPO ("
                      "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                      "name BLOB, "
                      "data BLOB, "
                      "keylocatorHash BLOB);\n "
                 , 0, 0, &errMsg);
    // Ignore errors (when database already exists, errors are expected)
    sqlite3_exec(m_db, "CREATE UNIQUE INDEX index_name ON NDN_REPO (name);");
  }
  else {
    NDN_LOG_DEBUG("Database file open failure rc:" << rc);
    BOOST_THROW_EXCEPTION(Error("Database file open failure"));
  }
  sqlite3_exec(m_db, "PRAGMA synchronous = OFF", 0, 0, &errMsg);
  sqlite3_exec(m_db, "PRAGMA journal_mode = WAL", 0, 0, &errMsg);
}

SqliteStorage::~SqliteStorage()
{
  sqlite3_close(m_db);
}

int64_t
SqliteStorage::insert(const Data& data)
{
  Name name = data.getFullName(); // store the full name

  if (name.empty()) {
    NDN_LOG_DEBUG("name is empty");
    return -1;
  }

  int rc = 0;

  sqlite3_stmt* insertStmt = 0;

  string insertSql = string("INSERT INTO NDN_REPO (id, name, data, keylocatorHash) "
                            "VALUES (?, ?, ?, ?)");

  if (sqlite3_prepare_v2(m_db, insertSql.c_str(), -1, &insertStmt, 0) != SQLITE_OK) {
    sqlite3_finalize(insertStmt);
    NDN_LOG_DEBUG("insert sql not prepared");
  }
  //Insert
  auto result = sqlite3_bind_null(insertStmt, 1);
  if (result == SQLITE_OK) {
    result = sqlite3_bind_blob(insertStmt, 2,
                               name.wireEncode().value(),
                               name.wireEncode().value_size(), SQLITE_STATIC);
  }
  if (result == SQLITE_OK) {
    result = sqlite3_bind_blob(insertStmt, 3,
                               data.wireEncode().wire(),
                               data.wireEncode().size(), SQLITE_STATIC);
  }
  if (result == SQLITE_OK) {
    ndn::ConstBufferPtr keyLocatorHash = computeKeyLocatorHash(data);
    BOOST_ASSERT(keyLocatorHash->size() == ndn::util::Sha256::DIGEST_SIZE);
    result = sqlite3_bind_blob(insertStmt, 4,
                               keyLocatorHash->data(),
                               keyLocatorHash->size(), SQLITE_STATIC);
  }

  int id = 0;
  if (result == SQLITE_OK) {
    rc = sqlite3_step(insertStmt);
    if (rc == SQLITE_CONSTRAINT) {
      NDN_LOG_DEBUG("Insert failed");
      sqlite3_finalize(insertStmt);
      BOOST_THROW_EXCEPTION(Error("Insert failed"));
     }
    sqlite3_reset(insertStmt);
    id = sqlite3_last_insert_rowid(m_db);
  }
  else {
    BOOST_THROW_EXCEPTION(Error("Some error with insert"));
  }

  sqlite3_finalize(insertStmt);
  return id;
}


bool
SqliteStorage::erase(const int64_t id)
{
  sqlite3_stmt* deleteStmt = 0;

  string deleteSql = string("DELETE from NDN_REPO where id = ? ;");

  if (sqlite3_prepare_v2(m_db, deleteSql.c_str(), -1, &deleteStmt, 0) != SQLITE_OK) {
    sqlite3_finalize(deleteStmt);
    NDN_LOG_DEBUG("delete statement prepared failed");
    BOOST_THROW_EXCEPTION(Error("delete statement prepared failed"));
  }

  if (sqlite3_bind_int64(deleteStmt, 1, id) == SQLITE_OK) {
    int rc = sqlite3_step(deleteStmt);
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
      NDN_LOG_DEBUG(" node delete error rc:" << rc);
      sqlite3_finalize(deleteStmt);
      BOOST_THROW_EXCEPTION(Error(" node delete error"));
    }
    if (sqlite3_changes(m_db) != 1)
      return false;
  }
  else {
    NDN_LOG_DEBUG("delete bind error" );
    sqlite3_finalize(deleteStmt);
    BOOST_THROW_EXCEPTION(Error("delete bind error"));
  }
  sqlite3_finalize(deleteStmt);
  return true;
}

shared_ptr<Data>
SqliteStorage::readData(int64_t id)
{
  sqlite3_stmt* queryStmt = 0;
  string sql = string("SELECT * FROM NDN_REPO WHERE id = ? ;");
  int rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &queryStmt, 0);
  if (rc == SQLITE_OK) {
    if (sqlite3_bind_int64(queryStmt, 1, id) == SQLITE_OK) {
      rc = sqlite3_step(queryStmt);
      if (rc == SQLITE_ROW) {
        auto data = make_shared<Data>();
        try {
          data->wireDecode(Block(reinterpret_cast<const uint8_t*>(sqlite3_column_blob(queryStmt, 2)),
                                 sqlite3_column_bytes(queryStmt, 2)));
        }
        catch (const ndn::Block::Error& error) {
          NDN_LOG_DEBUG(error.what());
          return nullptr;
        }
        sqlite3_finalize(queryStmt);
        NDN_LOG_DEBUG("Data from db: " << *data);
        return data;
      }
      else if (rc == SQLITE_DONE) {
        return nullptr;
      }
      else {
        NDN_LOG_DEBUG("Database query failure rc:" << rc );
        sqlite3_finalize(queryStmt);
        BOOST_THROW_EXCEPTION(Error("Database query failure"));
      }
    }
    else {
      NDN_LOG_DEBUG("select bind error");
      sqlite3_finalize(queryStmt);
      BOOST_THROW_EXCEPTION(Error("select bind error"));
    }
    sqlite3_finalize(queryStmt);
  }
  else {
    sqlite3_finalize(queryStmt);
    NDN_LOG_DEBUG("select statement prepared failed");
    BOOST_THROW_EXCEPTION(Error("select statement prepared failed"));
  }

  return nullptr;
}

shared_ptr<Data>
SqliteStorage::read(const Name& name)
{
  std::pair<int64_t, Name> res = find(name);

  if (res.first == 0)
    return nullptr;
  else {
    NDN_LOG_DEBUG("Found in database " << name << " " << res.first << " " << res.second);
    return readData(res.first);
  }
}

// shared_ptr<Data>
// SqliteStorage::read(int64_t id)
// {
//   return readData(id);
// }

bool
SqliteStorage::has(const Name& name)
{
  // find exact match
  std::pair<int64_t, Name> res = find(name, true);
  return (res.first == 0) ? false : true;
}

std::pair<int64_t, Name>
SqliteStorage::find(const Name& name, bool exactMatch)
{
  NDN_LOG_DEBUG("Trying to find: " << name);
  string sql;
  if (exactMatch)
    sql = "SELECT * FROM NDN_REPO WHERE name = ? ;";
  else
    sql = "SELECT * FROM NDN_REPO WHERE name >= ? order by name asc limit 1 ;";
    // sql = "SELECT * FROM NDN_REPO WHERE name >= ? and name < ?;";

  sqlite3_stmt* queryStmt = 0;
  int rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &queryStmt, 0);
  if (rc == SQLITE_OK) {
    auto result = sqlite3_bind_blob(queryStmt, 1,
                               name.wireEncode().value(),
                               name.wireEncode().value_size(), SQLITE_STATIC);
    // NDN_LOG_DEBUG("The name of next successor: " << name.getSuccessor());
    // this is the code for using getsuccessor to locate prefix match items
    // but it's not work, usually found the 0 result for the match
    // restore the code for future use

    // if ((result == SQLITE_OK) && !exactMatch) {
    //   // Use V in TLV for prefix match when there is no exact match
    //   result = sqlite3_bind_blob(queryStmt, 2,
    //                                name.getSuccessor().wireEncode().value(),
    //                                name.getSuccessor().wireEncode().value_size(), SQLITE_STATIC);
    //   NDN_LOG_DEBUG("result : " << result << " for name:"<< name);
    // }

    if (result == SQLITE_OK) {
      rc = sqlite3_step(queryStmt);
      if (rc == SQLITE_ROW) {
        Name foundName;

        const uint8_t* buffer = static_cast<const uint8_t*>(sqlite3_column_blob(queryStmt, 1));
        size_t nBytesLeft = sqlite3_column_bytes(queryStmt, 1);

        while (nBytesLeft > 0) {
          bool hasDecodingSucceeded;
          name::Component component;
          std::tie(hasDecodingSucceeded, component) = Block::fromBuffer(buffer, nBytesLeft);
          if (!hasDecodingSucceeded) {
            BOOST_THROW_EXCEPTION(Error("Error while decoding name from the database"));
          }
          foundName.append(component);
          buffer += component.size();
          nBytesLeft -= component.size();
        }
        NDN_LOG_DEBUG("Found: " << foundName << " " << sqlite3_column_int64(queryStmt, 0));
        if ((exactMatch && name == foundName) || (!exactMatch && name.isPrefixOf(foundName)))
          return std::make_pair(sqlite3_column_int64(queryStmt, 0), foundName);
      }
      else if (rc == SQLITE_DONE) {
        return std::make_pair(0, Name());
      }
      else {
        NDN_LOG_DEBUG("Database query failure rc:" << rc);
        sqlite3_finalize(queryStmt);
        BOOST_THROW_EXCEPTION(Error("Database query failure"));
      }
    }
    else {
      NDN_LOG_DEBUG("select bind error");
      sqlite3_finalize(queryStmt);
      BOOST_THROW_EXCEPTION(Error("select bind error"));
    }
    sqlite3_finalize(queryStmt);
  }
  else {
    sqlite3_finalize(queryStmt);
    NDN_LOG_DEBUG("select statement prepared failed");
    BOOST_THROW_EXCEPTION(Error("select statement prepared failed"));
  }
  return std::make_pair(0, Name());
}

int64_t
SqliteStorage::size()
{
  sqlite3_stmt* queryStmt = 0;
  string sql("SELECT count(*) FROM NDN_REPO ");
  int rc = sqlite3_prepare_v2(m_db, sql.c_str(), -1, &queryStmt, 0);
  if (rc != SQLITE_OK){
      NDN_LOG_DEBUG("Database query failure rc:" << rc);
      sqlite3_finalize(queryStmt);
      BOOST_THROW_EXCEPTION(Error("Database query failure"));
    }

  rc = sqlite3_step(queryStmt);
  if (rc != SQLITE_ROW){
      NDN_LOG_DEBUG("Database query failure rc:" << rc);
      sqlite3_finalize(queryStmt);
      BOOST_THROW_EXCEPTION(Error("Database query failure"));
    }

  int64_t nDatas = sqlite3_column_int64(queryStmt, 0);
  return nDatas;
}

} // namespace repo
