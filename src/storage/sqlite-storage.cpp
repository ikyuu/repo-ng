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
#include <ndn-cxx/util/sqlite3-statement.hpp>

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
                      "data BLOB);\n"
                 , 0, 0, &errMsg);
    // Ignore errors (when database already exists, errors are expected)
    sqlite3_exec(m_db, "CREATE UNIQUE INDEX index_name ON NDN_REPO (name);"
                 , 0, 0, &errMsg);
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

  string insertSql = string("INSERT INTO NDN_REPO (id, name, data) "
                            "VALUES (?, ?, ?)");
  ndn::util::Sqlite3Statement stmt(m_db, insertSql);

  //Insert
  auto result = sqlite3_bind_null(stmt.operator sqlite3_stmt*(), 1);
  if (result == SQLITE_OK) {
    result = stmt.bind(2, name.wireEncode().value(),
                          name.wireEncode().value_size(), SQLITE_STATIC);
  }
  if (result == SQLITE_OK) {
    result = stmt.bind(3, data.wireEncode().wire(),
                       data.wireEncode().size(), SQLITE_STATIC);
  }

  int id = 0;
  if (result == SQLITE_OK) {
    rc = stmt.step();
    if (rc == SQLITE_CONSTRAINT) {
      std::cout << "rc: " << rc << " SQLITE_CONSTRAINT:" << SQLITE_CONSTRAINT;
      NDN_LOG_DEBUG("Insert failed");
      BOOST_THROW_EXCEPTION(Error("Insert failed"));
     }
    sqlite3_reset(stmt.operator sqlite3_stmt*());
    id = sqlite3_last_insert_rowid(m_db);
  }
  else {
    BOOST_THROW_EXCEPTION(Error("Some error with insert"));
  }
  return id;
}

bool
SqliteStorage::erase(const Name& name)
{
  string deleteSql = "DELETE FROM NDN_REPO WHERE name = ? ;";
  ndn::util::Sqlite3Statement stmt(m_db, deleteSql);

  auto result = stmt.bind(1,
                          name.wireEncode().value(),
                          name.wireEncode().value_size(), SQLITE_STATIC);

  if (result == SQLITE_OK) {
    int rc = stmt.step();
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
      NDN_LOG_DEBUG(" node delete error rc:" << rc);
      BOOST_THROW_EXCEPTION(Error(" node delete error"));
    }
    if (sqlite3_changes(m_db) != 1){
      return false;
      }
  }
  else {
    NDN_LOG_DEBUG("delete bind error" );
    BOOST_THROW_EXCEPTION(Error("delete bind error"));
  }
  return true;
}

shared_ptr<Data>
SqliteStorage::read(const Name& name)
{
  std::pair<std::pair<int64_t, Name>, shared_ptr<Data>> res = find(name);

  if (res.first.first == 0)
    return nullptr;
  else {
    NDN_LOG_DEBUG("Found in database id: " << " " << res.first.first << " name: " << res.first.second);
    return res.second;
  }
}

bool
SqliteStorage::has(const Name& name)
{
  // find exact match
  std::pair<int64_t, Name> res = find(name, true).first;
  return (res.first == 0) ? false : true;
}

std::pair<std::pair<int64_t, Name>, shared_ptr<Data>>
SqliteStorage::find(const Name& name, bool exactMatch)
{
  NDN_LOG_DEBUG("Trying to find: " << name);
  Name name_successor = name.getSuccessor();

  string sql;
  if (exactMatch)
    sql = "SELECT * FROM NDN_REPO WHERE name = ? ;";
  else
    sql = "SELECT * FROM NDN_REPO WHERE name >= ? and name < ?;";

  ndn::util::Sqlite3Statement stmt(m_db, sql);

  auto result = stmt.bind(1,
                          name.wireEncode().value(),
                          name.wireEncode().value_size(), SQLITE_STATIC);

  // this is the code for using getsuccessor to locate prefix match items
  if ((result == SQLITE_OK) && !exactMatch) {
    // Use V in TLV for prefix match when there is no exact match
    result = stmt.bind(2,
                    name_successor.wireEncode().value(),
                    name_successor.wireEncode().value_size(), SQLITE_STATIC);
    }
  NDN_LOG_DEBUG("The name of next successor: " << name.getSuccessor());

  if (result == SQLITE_OK) {
    int rc = stmt.step();
    if (rc == SQLITE_ROW) {
      Name foundName;
      // const uint8_t* buffer = static_cast<const uint8_t*>(sqlite3_column_blob(queryStmt, 1));
      // size_t nBytesLeft = sqlite3_column_bytes(queryStmt, 1);

      // while (nBytesLeft > 0) {
      //   bool hasDecodingSucceeded;
      //   name::Component component;
      //   std::tie(hasDecodingSucceeded, component) = Block::fromBuffer(buffer, nBytesLeft);
      //   if (!hasDecodingSucceeded) {
      //     BOOST_THROW_EXCEPTION(Error("Error while decoding name from the database"));
      //   }
      //   foundName.append(component);
      //   buffer += component.size();
      //   nBytesLeft -= component.size();
      // }

      NDN_LOG_DEBUG("sql Found: " << foundName << " " << stmt.getInt(0));

      auto data = make_shared<Data>();
      try {
        data->wireDecode(Block(reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt.operator sqlite3_stmt*(), 2)),
                                sqlite3_column_bytes(stmt.operator sqlite3_stmt*(), 2)));
      }
      catch (const ndn::Block::Error& error) {
        NDN_LOG_DEBUG(error.what());
        return std::make_pair(std::make_pair(0, Name()), nullptr);;
      }
      NDN_LOG_DEBUG("Data from db: " << *data);

      foundName = data->getFullName();

      if ((exactMatch && name == foundName) || (!exactMatch && name.isPrefixOf(foundName)))
        return std::make_pair(std::make_pair(stmt.getInt(0), foundName), data);
    }
    else if (rc == SQLITE_DONE) {
      return std::make_pair(std::make_pair(0, Name()), make_shared<Data>());
    }
    else {
      NDN_LOG_DEBUG("Database query failure rc:" << rc);
      BOOST_THROW_EXCEPTION(Error("Database query failure"));
    }
  }
  else {
    NDN_LOG_DEBUG("select bind error");
    BOOST_THROW_EXCEPTION(Error("select bind error"));
  }
  return std::make_pair(std::make_pair(0, Name()), nullptr);
}

int64_t
SqliteStorage::size()
{
  string sql("SELECT count(*) FROM NDN_REPO ");
  ndn::util::Sqlite3Statement stmt(m_db, sql);

  int rc = stmt.step();
  if (rc != SQLITE_ROW){
      NDN_LOG_DEBUG("Database query failure rc:" << rc);
      BOOST_THROW_EXCEPTION(Error("Database query failure"));
    }

  int64_t nDatas = stmt.getInt(0);
  return nDatas;
}

} // namespace repo
