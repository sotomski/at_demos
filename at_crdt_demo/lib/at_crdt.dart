import 'dart:async';
import 'dart:convert';

import 'package:at_client/at_client.dart';
import 'package:crdt/crdt.dart';
import 'package:crdt/map_crdt.dart';
import 'package:uuid/uuid.dart';
import 'package:uuid/validation.dart';

/// QUESTIONS QUESTIONS QUESTIONS
/// - How to handle the nodeId? Should it be a parameter to the CRDT?
/// - Is it possible to integrate some of CRDT record properties (ie. node id) with the atPlatform?
/// - Since every [Record] has it's own HLC, do we have to use UUIDs as keys?
/// - Does it make sense to cache records here or is atClient efficient enough?

/// HIDDEN KNOWLEDGE 🧠
/// - Karol: I've tried using [MapCrdtBase] as to implement the CRDT,
/// but its sync API clashed with AtClient async API.

// TODO: Consider performance of this solution. [MapCrdtBase] is deemed inefficient by its author.
/// A state-based grow-only CRDT implementation.
class AtCrdt extends Crdt {
  final AtClient atClient;
  final String name;
  final Set<String> tables;
  final String? sharedWith;
  final String? sharedBy;

  // TODO: Uuid v1 assumed here! Check the assumption against privacy concerns.
  /// The [tables] names to use for the keys in the CRDT
  /// Example: 123e4567-e89b-12d3-a456-426655440000.table.crdt.app@atSign
  /// Warning! Beware the max namespace length in atProtocol is 55 - 36 = 19
  AtCrdt._({
    required this.atClient,
    this.name = 'crdt',
    required Iterable<String> tables,
    this.sharedBy,
    this.sharedWith,
  })  : assert(tables.isNotEmpty, "Tables must not be empty"),
        assert(tables.length == tables.toSet().length,
            "Table names must be unique"),
        assert(tables.toList().every((t) => t.length <= 19 - name.length),
            "Table names must be at most ${19 - name.length} characters long"),
        // assert(
        //     (sharedBy != null && sharedWith != null) ||
        //         (sharedBy == null && sharedWith == null),
        //     "Either both sharedBy and sharedWith must be provided (shared) or none (own)"),
        tables = tables.toSet();

  factory AtCrdt.self({
    required AtClient atClient,
    required Iterable<String> tables,
    String name = 'crdt',
  }) {
    return AtCrdt._(
      atClient: atClient,
      name: name,
      tables: tables,
      sharedWith: null,
      sharedBy: atClient.getCurrentAtSign(),
    );
  }

  factory AtCrdt.sharedWithOther({
    required AtClient atClient,
    required Iterable<String> tables,
    String name = 'crdt',
    required String otherAtSign,
  }) {
    return AtCrdt._(
      atClient: atClient,
      name: name,
      tables: tables,
      sharedWith: otherAtSign,
      sharedBy: atClient.getCurrentAtSign(),
    );
  }

  factory AtCrdt.sharedByOther({
    required AtClient atClient,
    required Iterable<String> tables,
    String name = 'crdt',
    required String otherAtSign,
  }) {
    return AtCrdt._(
      atClient: atClient,
      name: name,
      tables: tables,
      sharedWith: atClient.getCurrentAtSign(),
      sharedBy: otherAtSign,
    );
  }

  Future<void> init() async {
    late String nodeId;

    if (await isEmpty()) {
      nodeId = generateNodeId();
    } else {
      for (var table in tables) {
        final records = await getRecords(table);
        if (records.isNotEmpty) {
          nodeId = records.values.first.modified.nodeId;
          break;
        }
      }
    }

    canonicalTime = Hlc.zero(nodeId);
    canonicalTime = await getLastModified();
  }

  Future<bool> isEmpty() async {
    return Future.wait(tables.map((tableName) =>
            _getAtKeys(tableName).then((keyList) => keyList.isEmpty)))
        .then((emptyStatusList) =>
            emptyStatusList.any((isTableEmpty) => isTableEmpty));
  }

  @override
  FutureOr<CrdtChangeset> getChangeset(
      {Iterable<String>? onlyTables,
      String? onlyNodeId,
      String? exceptNodeId,
      Hlc? modifiedOn,
      Hlc? modifiedAfter}) async {
    assert(onlyNodeId == null || exceptNodeId == null);
    assert(modifiedOn == null || modifiedAfter == null);

    // Modified times use the local node id
    modifiedOn = modifiedOn?.apply(nodeId: nodeId);
    modifiedAfter = modifiedAfter?.apply(nodeId: nodeId);

    // Ensure all incoming tables exist in local dataset
    onlyTables ??= tables;
    final badTables = onlyTables.toSet().difference(tables);
    if (badTables.isNotEmpty) {
      throw 'Unknown table(s): ${badTables.join(', ')}';
    }

    // Get records for the specified tables
    final changesetPerTableFuture = onlyTables.map((table) {
      return getRecords(table).then((tableRecords) => tableRecords
        // Apply remaining filters
        ..removeWhere((_, value) =>
            (onlyNodeId != null && value.hlc.nodeId != onlyNodeId) ||
            (exceptNodeId != null && value.hlc.nodeId == exceptNodeId) ||
            (modifiedOn != null && value.modified != modifiedOn) ||
            (modifiedAfter != null && value.modified <= modifiedAfter)));
    });

    final changesetPerTable = await Future.wait(changesetPerTableFuture);
    final changeset = Map.fromIterables(
      onlyTables,
      changesetPerTable,
    );

    // Remove empty table changesets
    changeset.removeWhere((_, records) => records.isEmpty);

    return changeset.map((table, records) => MapEntry(
        table,
        records
            .map((key, record) => MapEntry(key, {
                  'key': _crdtKeyFrom(key),
                  ...record.toJson(),
                }))
            .values
            .toList()));
  }

  @override
  FutureOr<Hlc> getLastModified({
    String? onlyNodeId,
    String? exceptNodeId,
  }) async {
    assert(onlyNodeId == null || exceptNodeId == null);

    final modificationTimesFutures = tables.map((table) {
      return getRecords(table).then((tableRecords) => tableRecords.entries
          .map((e) => e.value)
          .where((r) =>
              (onlyNodeId == null && exceptNodeId == null) ||
              (onlyNodeId != null && r.hlc.nodeId == onlyNodeId) ||
              (exceptNodeId != null && r.hlc.nodeId != exceptNodeId))
          // Get only modified times
          .map((e) => e.modified));
    });

    final allModifiedResolved = await Future.wait(modificationTimesFutures)
        .then((timesPerTable) => timesPerTable.expand((times) => times));
    // Get highest time
    return allModifiedResolved.fold<Hlc>(
        Hlc.zero(nodeId), (p, e) => p > e ? p : e);
  }

  @override
  FutureOr<void> merge(CrdtChangeset changeset) async {
    if (changeset.recordCount == 0) return;

    // Ensure all incoming tables exist in local dataset
    final badTables = changeset.keys.toSet().difference(tables);
    if (badTables.isNotEmpty) {
      throw 'Unknown table(s): ${badTables.join(', ')}';
    }

    // Ignore empty records
    changeset.removeWhere((_, records) => records.isEmpty);

    // Validate changeset and get new canonical time
    final hlc = validateChangeset(changeset);

    final newRecords = <String, Map<String, Record>>{};
    for (final entry in changeset.entries) {
      final table = entry.key;
      for (final record in entry.value) {
        final existing = await getRecord(table, record['key'] as String);
        if (existing == null || record['hlc'] as Hlc > existing.hlc) {
          newRecords[table] ??= {};
          newRecords[table]![record['key'] as String] = Record(
            record['value'],
            record['is_deleted'] as bool,
            record['hlc'] as Hlc,
            hlc,
          );
        }
      }
    }

    // Write new records
    await putRecords(newRecords);
    onDatasetChanged(changeset.keys, hlc);
  }

  FutureOr<Record?> getRecord(String table, String key) async {
    try {
      final atKey = _atKeyFrom(table, UuidValue.fromString(key));
      final atValue = await atClient.get(atKey);
      return _recordFromJson(atValue.value);
    } catch (e) {
      return null;
    }
  }

  Future<Map<AtKey, Record>> getRecords(String table) async {
    final recordFutures =
        await _getAtKeys(table).then((keys) => keys.map((key) async {
              final value = await atClient.get(key);
              return (key, value);
            }));
    final allRecords = await Future.wait(recordFutures);
    return {for (var e in allRecords) e.$1: _recordFromJson(e.$2.value)};
  }

  // TODO: Consider using AtKey for the dataset (CRUD on records).
  // TODO: To get an atKey, not only its key but also its type (self|shared) is needed.
  FutureOr<void> putRecords(Map<String, Map<String, Record>> dataset) async {
    for (final entry in dataset.entries) {
      final tableName = entry.key;

      for (final recordEntry in entry.value.entries) {
        final crdtKey = recordEntry.key;
        final record = recordEntry.value;
        // TODO:  ??? Is this correct? Why isn't Record.toJson serialize modified?
        final recordJson = jsonEncode({
          'value': record.value,
          'is_deleted': record.isDeleted,
          'hlc': record.hlc.toString(),
          'modified': record.modified.toString(),
        });
        final atKey = _atKeyFrom(tableName, UuidValue.fromString(crdtKey));
        await atClient.put(atKey, recordJson);
      }
    }
  }

  /// Get a value from the local dataset.
  Future<dynamic> get(String table, String key) async {
    if (!tables.contains(table)) throw 'Unknown table: $table';
    final value = await getRecord(table, key);
    return value == null || value.isDeleted ? null : value.value;
  }

  /// Get a table map from the local dataset.
  Future<Map<String, dynamic>> getMap(String table) async {
    if (!tables.contains(table)) throw 'Unknown table: $table';
    return (await getRecords(table)
          ..removeWhere((_, record) => record.isDeleted))
        .map((key, record) => MapEntry(_crdtKeyFrom(key), record.value));
  }

  /// Insert a single value into this dataset.
  ///
  /// Use [putAll] if inserting multiple values to avoid incrementing the
  /// canonical time unnecessarily.
  Future<void> put(String table, String key, dynamic value,
          [bool isDeleted = false]) =>
      putAll({
        table: {key: value}
      }, isDeleted);

  /// Insert multiple values into this dataset.
  Future<void> putAll(Map<String, Map<String, dynamic>> dataset,
      [bool isDeleted = false]) async {
    // Ensure all incoming tables exist in local dataset
    final badTables = dataset.keys.toSet().difference(tables);
    if (badTables.isNotEmpty) {
      throw 'Unknown table(s): ${badTables.join(', ')}';
    }

    // Ignore empty records
    dataset.removeWhere((_, records) => records.isEmpty);

    // Generate records with incremented canonical time
    final hlc = canonicalTime.increment();
    final records = dataset.map((table, values) => MapEntry(
        table,
        values.map((key, value) =>
            MapEntry(key, Record(value, isDeleted, hlc, hlc)))));

    // Store records
    await putRecords(records);
    onDatasetChanged(records.keys, hlc);
  }

  // TODO: Add observer pattern to the CRDT
  // Stream<WatchEvent> watch(String table, {String? key}) {
  //   // TODO: implement watch
  //   throw UnimplementedError();
  // }

  Future<void> delete() async {
    final keysToDelete =
        await Future.wait(tables.map((table) => _getAtKeys(table)));
    keysToDelete.expand((key) => key).forEach((key) async {
      await atClient.delete(key);
    });
  }

  Future<List<AtKey>> _getAtKeys(String table) async {
    final ns = atClient.getPreferences()!.namespace ?? '';
    final pattern = r'.*' +
        RegExp.escape(table) +
        r'\.' +
        RegExp.escape(name) +
        r'\.' +
        RegExp.escape(ns) +
        r'.*';

    if ([null, atClient.getCurrentAtSign()].contains(sharedBy) &&
        sharedWith == null) {
      // Private/Self
      final keys = await atClient.getAtKeys(
        regex: pattern,
      );

      // print(
      //   'atClient(${atClient.getCurrentAtSign()})::getAtKeys($table) BEFORE REMOVAL => $keys',
      // );

      // We only want private keys. Get rid of shared keys.
      keys.removeWhere((element) =>
          element.sharedWith != atClient.getCurrentAtSign() &&
          element.sharedWith != null);

      // print(
      //   'atClient(${atClient.getCurrentAtSign()})::getAtKeys($table) AFTER REMOVAL => $keys',
      // );

      return keys;
    } else if ([null, atClient.getCurrentAtSign()].contains(sharedBy) &&
        sharedWith != null &&
        sharedWith != atClient.getCurrentAtSign()) {
      // Shared with other
      final keys = await atClient.getAtKeys(
        regex: pattern,
        // sharedBy: sharedBy,
        sharedWith: sharedWith,
      );

      // print(
      //   'atClient(sharedBy: $sharedBy, sharedWith: $sharedWith)::getAtKeys($table) => $keys',
      // );

      return keys;
    } else if (sharedBy != null && sharedBy != atClient.getCurrentAtSign()) {
      // Shared by other
      final keys = await atClient.getAtKeys(
        regex: pattern,
        sharedBy: sharedBy,
        // sharedWith: sharedWith,
      );

      // print(
      //   'atClient(sharedBy: $sharedBy, sharedWith: $sharedWith)::getAtKeys($table) => $keys',
      // );

      return keys;
    }

    throw 'Unsupported combination of sharedBy ($sharedBy) and sharedWith ($sharedWith)';
  }

  Record _recordFromJson(String jsonString) {
    final json = jsonDecode(jsonString, reviver: (key, value) {
      if (key == 'hlc' || key == 'modified') {
        return Hlc.parse(value as String);
      }
      return value;
    });
    return Record(
      json['value'],
      json['is_deleted'],
      json['hlc'],
      json['modified'] ?? json['hlc'],
    );
  }

  AtKey _atKeyFrom(String table, UuidValue key) {
    final ns = atClient.getPreferences()!.namespace;
    return sharedWith == null
        ? AtKey.self(
            "${key.uuid}.$table.$name",
            namespace: ns,
            sharedBy: atClient.getCurrentAtSign()!,
          ).build()
        : (AtKey.shared(
            "${key.uuid}.$table.$name",
            namespace: ns,
            sharedBy: sharedBy!,
          )..sharedWith(sharedWith!))
            .build();
  }

  String _crdtKeyFrom(AtKey atKey) {
    final recordKey = atKey.key.split('.').first;
    if (!UuidValidation.isValidUUID(fromString: recordKey)) {
      throw 'Invalid record key: $recordKey';
    }
    return recordKey;
  }
}
