// Copyright (c) 2015, the Dogma Project Authors.
// Please see the AUTHORS file for details. All rights reserved.
// Use of this source code is governed by a zlib license that can be found in
// the LICENSE file.

/// An implementation of a Dogma [Connection] that interfaces with a PostgreSQL database.
library dogma.postgresql_connection;

//---------------------------------------------------------------------
// Standard libraries
//---------------------------------------------------------------------

import 'dart:async';

//---------------------------------------------------------------------
// Imports
//---------------------------------------------------------------------

import 'package:dogma_connection/credentials.dart';
import 'package:dogma_sql_connection/sql_connection.dart';
import 'package:dogma_sql_connection/src/sql_schema.dart';
import 'package:dogma_sql_connection/src/sql_statement_builder.dart';
import 'package:postgresql/pool.dart';
import 'package:postgresql/postgresql.dart' as postgres show Connection;

//---------------------------------------------------------------------
// Library contents
//---------------------------------------------------------------------

/// An implementation of [SqlConnection] targeting a PostgreSQL database.
class PostgreSqlConnection extends SqlConnection {
  //---------------------------------------------------------------------
  // Class variables
  //---------------------------------------------------------------------

  /// The default port for communicating with a PostgreSQL server.
  static const int defaultPort = 5432;

  //---------------------------------------------------------------------
  // Member variables
  //---------------------------------------------------------------------

  /// The name of the host of the PostgreSQL database.
  final String host;
  /// The name of the database.
  final String database;
  /// The port to connect on.
  final int port;
  /// The query parameters.
  final Map<String, String> queryParameters;
  /// The minimum number of connections to open.
  final int minConnections;
  /// The maximum number of connections to open.
  final int maxConnections;

  /// The pool of connections to the PostgreSQL database.
  Pool _connectionPool;

  //---------------------------------------------------------------------
  // Construction
  //---------------------------------------------------------------------

  /// Creates an instance of the [PostgreSqlConnection] targeting the given [host] and [database].
  ///
  /// The [port] can be specified if the database is not connected to the
  /// default. Additionally [queryParameters] can be specified to further
  /// configure the connection uri.
  ///
  /// The [PostgresSqlConnection] internally utilizes a connection pool to
  /// communicate with the database. The [minConnections] and [maxConnections]
  /// value allows this to be configured.
  PostgreSqlConnection(this.host,
                       this.database,
                      {this.port: defaultPort,
                       this.queryParameters,
                       this.minConnections: 2,
                       this.maxConnections: 5})
      : super(new SqlSchema(), new SqlStatementBuilder())
  {
    schema.connection = this;
  }

  //---------------------------------------------------------------------
  // Connection
  //---------------------------------------------------------------------

  @override
  Future<Null> open(Credentials credentials) async {
    if (credentials is! NetworkCredentials) {
      throw new ArgumentError('Expecting NetworkCredentials for the connection');
    }

    var networkCredentials = credentials as NetworkCredentials;

    // Generate the URI for the connection
    var uri = new Uri(
        scheme: 'postgres',
        userInfo: '${networkCredentials.userName}:${networkCredentials.password}',
        host: host,
        port: port,
        path: database,
        queryParameters: queryParameters
    );

    // Create the connection pool
    _connectionPool = new Pool(
        uri.toString(),
        minConnections: minConnections,
        maxConnections: maxConnections
    );

    // Start up the pool
    await _connectionPool.start();
  }

  //---------------------------------------------------------------------
  // SqlConnection
  //---------------------------------------------------------------------

  @override
  Stream<dynamic> executeSql(String statement) async {
    assert(_connectionPool != null);

    print(statement);

    // Get the connection from the pool
    var connection = await _connectionPool.connect() as postgres.Connection;

    // Get the values
    var values = connection.query(statement).map((row) => row.toList());

    // Return the connection to the pool
    connection.close();

    return values;
  }
}

/// Connect to a PostgreSQL [database] at the [host] using the credentials for [userName] with [password].
Future<PostgreSqlConnection> connect(String host,
                                     String database,
                                     String userName,
                                     String password,
                                    {int port: PostgreSqlConnection.defaultPort,
                                     Map<String, String> queryParameters}) async
{
  var connection = new PostgreSqlConnection(
      host,
      database,
      port: port,
      queryParameters: queryParameters
  );

  var credentials = new NetworkCredentials(userName, password);

  await connection.open(credentials);

  return connection;
}
