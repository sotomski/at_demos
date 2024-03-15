import 'dart:async';

import 'package:at_client/at_client.dart';
import 'package:crdt/crdt.dart';
import 'package:crdt/map_crdt.dart';
import 'package:uuid/validation.dart';

/// QUESTIONS QUESTIONS QUESTIONS
/// - How to handle the nodeId? Should it be a parameter to the CRDT?
/// - Is it possible to integrate some of CRDT record properties (ie. node id) with the atPlatform?
/// - Since every [Record] has it's own HLC, do we have to use UUIDs as keys?
/// - Does it make sense to cache records here or is atClient efficient enough?

/// HIDDEN KNOWLEDGE 🧠
/// - Karol: I've tried using [MapCrdtBase] as a first approach to implement the CRDT,
/// but it's sync API clashed with AtClient async API.

// TODO: Consider performance of this solution. [MapCrdtBase] is deemed inefficient by its author.
/// A state-based grow-only CRDT implementation.
class AtCrdt extends Crdt {
  final AtClient atClient;
  final Set<String> tables;

  // TODO: Uuid v1 assumed here! Check the assumption against privacy concerns.
  /// The [tables] names to use for the keys in the CRDT
  /// Example: 123e4567-e89b-12d3-a456-426655440000.table.crdt.app@atSign
  /// Warning! Beware the max namespace length in atProtocol is 55 - 36 = 19
  AtCrdt({required this.atClient, required Iterable<String> tables})
      : assert(tables.isNotEmpty, "Tables must not be empty"),
        assert(tables.length == tables.toSet().length,
            "Table names must be unique"),
        assert(tables.toList().every((t) => t.length <= 19),
            "Table names must be at most 19 characters long"),
        tables = tables.toSet();

  Future<void> initialize() async {
    late String nodeId;
    for (var table in tables) {
      final records = await getRecords(table);
      if (records.isNotEmpty) {
        nodeId = records.values.first.modified.nodeId;
        break;
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

    // TODO: Copied verbatim from [MapCrdtBase]. Looks fishy 🐠.
    return changeset.map((table, records) => MapEntry(
        table,
        records
            .map((key, record) => MapEntry(key, {
                  'key': key.crdtRecordKey(),
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
    final atKey = SelfKey()
      ..key = "$key.$table"
      ..namespace = atClient.getPreferences()!.namespace;
    final atValue = await atClient.get(atKey);
    return atValue.value as Record;
  }

  Future<Map<AtKey, Record>> getRecords(String table) async {
    final recordFutures =
        await _getAtKeys(table).then((keys) => keys.map((key) async {
              final value = await atClient.get(key);
              return (key, value);
            }));
    final allRecords = await Future.wait(recordFutures);
    return {for (var e in allRecords) e.$1: e.$2.value as Record};
  }

  FutureOr<void> putRecords(Map<String, Map<String, Record>> dataset) async {
    for (final entry in dataset.entries) {
      final tableName = entry.key;

      for (final recordEntry in entry.value.entries) {
        final crdtKey = recordEntry.key;
        final record = recordEntry.value;
        final atKey = AtKey()
          ..key = "$crdtKey.$tableName"
          ..namespace = atClient.getPreferences()!.namespace;

        await atClient.put(atKey, record);
      }
    }
  }

  // @override
  // Stream<WatchEvent> watch(String table, {String? key}) {
  //   // TODO: implement watch
  //   throw UnimplementedError();
  // }

  Future<List<AtKey>> _getAtKeys(String table) async {
    final ns = atClient.getPreferences()!.namespace;
    return atClient.getAtKeys(regex: "*.$table.crdt.$ns");
  }
}

extension _AtKeyCrdt on AtKey {
  String crdtRecordKey() {
    final recordKey = key.split('.').first;
    if (!UuidValidation.isValidUUID(fromString: recordKey)) {
      throw 'Invalid record key: $recordKey';
    }
    return recordKey;
  }
}
