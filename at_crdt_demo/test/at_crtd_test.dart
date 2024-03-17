// ignore_for_file: unawaited_futures

import 'dart:async';

import 'package:at_client/at_client.dart';
import 'package:at_crdt_demo/at_crdt.dart';
import 'package:crdt/crdt.dart';
import 'package:mocktail/mocktail.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

Future<void> get _delay => Future.delayed(Duration(milliseconds: 1));

/// These tests are heavily inspired by Hive CRDT tests
/// https://github.com/cachapa/hive_crdt/blob/master/test/hive_crdt_test.dart
void main() {
  late AtCrdt crdt;
  final FakeAtClient atClient = FakeAtClient();

  tearDown(() => atClient.data.clear());

  Future<AtCrdt> createCrdt(Set<String> tables,
      {String? sharedBy, String? sharedWith}) async {
    final aCrdt = AtCrdt(
      atClient: atClient,
      tables: tables,
      sharedBy: sharedBy,
      sharedWith: sharedWith,
    );
    await aCrdt.init();
    return aCrdt;
  }

  group('Empty', () {
    setUp(() async {
      crdt = await createCrdt({'table'});
    });

    test('Node ID', () {
      expect(Uuid.isValidUUID(fromString: crdt.nodeId), true);
    });

    test('Empty', () async {
      expect(crdt.canonicalTime, Hlc.zero(crdt.nodeId));
      expect(await crdt.isEmpty(), true);
    });
  });

  group('Insert', () {
    setUp(() async {
      crdt = await createCrdt({'table'});
    });

    test('Single', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1);
      expect(await crdt.isEmpty(), false);
      expect((await crdt.getChangeset()).recordCount, 1);
      expect(await crdt.get('table', key), 1);
    });

    test('Null', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, null);
      expect((await crdt.getChangeset()).recordCount, 1);
      expect(await crdt.get('table', key), null);
    });

    test('Update', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1);
      await crdt.put('table', key, 2);
      expect((await crdt.getChangeset()).recordCount, 1);
      expect(await crdt.get('table', key), 2);
    });

    test('Multiple', () async {
      final key1 = Uuid().v1();
      final key2 = Uuid().v1();
      await crdt.putAll({
        'table': {key1: 1, key2: 2}
      });
      expect((await crdt.getChangeset()).recordCount, 2);
      expect(await crdt.getMap('table'), {key1: 1, key2: 2});
    });

    test('Enforce table existence', () {
      final key = Uuid().v1();
      expect(() async => await crdt.put('not_test', key, 1),
          throwsA('Unknown table(s): not_test'));
    });
  });

  group('Delete', () {
    setUp(() async {
      crdt = await createCrdt({'table'});
    });

    test('Set deleted', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1, true);
      expect(await crdt.isEmpty(), false);
      expect((await crdt.getChangeset()).recordCount, 1);
      expect((await crdt.getMap('table')).length, 0);
      expect(await crdt.get('table', key), null);
    });

    test('Undelete', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1, true);
      await crdt.put('table', key, 1, false);
      expect(await crdt.isEmpty(), false);
      expect((await crdt.getChangeset()).recordCount, 1);
      expect((await crdt.getMap('table')).length, 1);
      expect(await crdt.get('table', key), 1);
    });
  });

  group('Merge', () {
    late AtCrdt crdt1;

    setUp(() async {
      crdt = await createCrdt({'table'});
      crdt1 = await createCrdt({'table'}, sharedWith: '@bob');
    });

    test('Into empty', () async {
      final key = Uuid().v1();
      await crdt1.put('table', key, 2);
      await crdt.merge(await crdt1.getChangeset());
      expect(await crdt.get('table', key), 2);
      expect(atClient.data.length, 2,
          reason: 'There should be 2 atRecords after merge');
    });

    test('Empty changeset', () async {
      final key = Uuid().v1();
      await crdt1.put('table', key, 2);
      await crdt.merge(await crdt1.getChangeset());
      expect(await crdt.get('table', key), 2);
    });

    test('Older', () async {
      final key = Uuid().v1();
      await crdt1.put('table', key, 2);
      await _delay;
      await crdt.put('table', key, 1);
      await crdt.merge(await crdt1.getChangeset());
      expect(await crdt.get('table', key), 1);
      expect(atClient.data.length, 2,
          reason: 'There should be 2 atRecords after merge');
    });

    test('Newer', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1);
      await _delay;
      await crdt1.put('table', key, 2);
      await crdt.merge(await crdt1.getChangeset());
      expect(await crdt.get('table', key), 2);
      expect(atClient.data.length, 2,
          reason: 'There should be 2 atRecords after merge');
    });

    test('Lower node id', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1);
      final changeset = await crdt.getChangeset();
      changeset['table']!.first.addAll({
        'hlc': (changeset['table']!.first['hlc'] as Hlc)
            .apply(nodeId: '00000000-0000-0000-0000-000000000000'),
        'value': 2,
      });
      await crdt.merge(changeset);
      expect(await crdt.get('table', key), 1);
    });

    test('Higher node id', () async {
      final key = Uuid().v1();
      await crdt.put('table', key, 1);
      final changeset = await crdt.getChangeset();
      changeset['table']!.first.addAll({
        'hlc': (changeset['table']!.first['hlc'] as Hlc)
            .apply(nodeId: 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
        'value': 2,
      });
      await crdt.merge(changeset);
      expect(await crdt.get('table', key), 2);
    });

    test('Enforce table existence', () async {
      final key = Uuid().v1();
      final other = await createCrdt({'not_table'}, sharedWith: '@john');
      await other.put('not_table', key, 1);
      expect(() async => await crdt.merge(await other.getChangeset()),
          throwsA('Unknown table(s): not_table'));
    });

    test('Update canonical time after merge', () async {
      final key = Uuid().v1();
      await crdt1.put('table', key, 2);
      await crdt.merge(await crdt1.getChangeset());
      expect(
          crdt.canonicalTime, crdt1.canonicalTime.apply(nodeId: crdt.nodeId));
    });
  });

  group('Changesets', () {
    late AtCrdt crdt1;
    late AtCrdt crdt2;

    setUp(() async {
      crdt = await createCrdt({'table'}, sharedWith: '@alice');
      crdt1 = await createCrdt({'table'}, sharedWith: '@bob');
      crdt2 = await createCrdt({'table'}, sharedWith: '@charlie');

      await crdt.put('table', Uuid().v1(), 1);
      await _delay;
      await crdt1.put('table', Uuid().v1(), 1);
      await _delay;
      await crdt2.put('table', Uuid().v1(), 1);

      await crdt.merge(await crdt1.getChangeset());
      await crdt.merge(await crdt2.getChangeset());
    });

    test('Tables', () async {
      final crdt3 =
          await createCrdt({'table', 'another_table'}, sharedWith: '@dave');
      await crdt3.put('another_table', Uuid().v1(), 1);
      final changeset = await crdt3.getChangeset(onlyTables: ['another_table']);
      expect(changeset.keys, ['another_table']);
    });

    test('After HLC', () async {
      expect(await crdt.getChangeset(modifiedAfter: crdt1.canonicalTime),
          await crdt2.getChangeset());
    });

    test('Empty changeset', () async {
      expect(await crdt.getChangeset(modifiedAfter: crdt2.canonicalTime), {});
    });

    test('At HLC', () async {
      final changeset =
          await crdt.getChangeset(modifiedOn: crdt1.canonicalTime);
      expect(changeset, await crdt1.getChangeset());
    });

    test('Only node id', () async {
      final changeset = await crdt.getChangeset(onlyNodeId: crdt1.nodeId);
      expect(changeset, await crdt1.getChangeset());
    });

    test('Except node id', () async {
      final originalChangeset = await crdt1.getChangeset();
      await crdt1.merge(await crdt2.getChangeset());
      final changeset = await crdt1.getChangeset(exceptNodeId: crdt2.nodeId);
      expect(changeset, originalChangeset);
    });
  });

  group('Last modified', () {
    late AtCrdt crdt1;
    late AtCrdt crdt2;

    setUp(() async {
      crdt = await createCrdt({'table'}, sharedWith: '@alice');
      crdt1 = await createCrdt({'table'}, sharedWith: '@bob');
      crdt2 = await createCrdt({'table'}, sharedWith: '@charlie');

      await crdt.put('table', Uuid().v1(), 1);
      await _delay;
      await crdt1.put('table', Uuid().v1(), 1);
      await _delay;
      await crdt2.put('table', Uuid().v1(), 1);

      await crdt.merge(await crdt1.getChangeset());
      await crdt.merge(await crdt2.getChangeset());
    });

    test('Everything', () async {
      expect(await crdt.getLastModified(),
          crdt2.canonicalTime.apply(nodeId: crdt.nodeId));
    });

    test('Only node id', () async {
      expect(await crdt.getLastModified(onlyNodeId: crdt1.nodeId),
          crdt1.canonicalTime.apply(nodeId: crdt.nodeId));
    });

    test('Except node id', () async {
      // Move canonical time forward in crdt
      await _delay;
      await crdt.put('table', Uuid().v1(), 1);
      expect(await crdt.getLastModified(exceptNodeId: crdt.nodeId),
          crdt2.canonicalTime.apply(nodeId: crdt.nodeId));
    });

    test('Assert exclusive parameters', () {
      expect(
          () async => await crdt.getLastModified(
              onlyNodeId: crdt.nodeId, exceptNodeId: crdt.nodeId),
          throwsA(isA<AssertionError>()));
    });
  });

  group('Tables changed stream', () {
    setUp(() async {
      crdt = await createCrdt({'table_1', 'table_2'}, sharedWith: '@alice');
    });

    test('Single change', () async {
      expectLater(
          crdt.onTablesChanged.map((e) => e.tables), emits(['table_1']));
      await crdt.put('table_1', Uuid().v1(), 1);
    });

    test('Multiple changes to same table', () async {
      expectLater(
          crdt.onTablesChanged.map((e) => e.tables), emits(['table_1']));
      await crdt.putAll({
        'table_1': {
          Uuid().v1(): 1,
          Uuid().v1(): 2,
        }
      });
    });

    test('Multiple tables', () async {
      expectLater(crdt.onTablesChanged.map((e) => e.tables),
          emits(['table_1', 'table_2']));
      await crdt.putAll({
        'table_1': {Uuid().v1(): 1},
        'table_2': {Uuid().v1(): 2},
      });
    });

    test('Do not notify empty changes', () async {
      expectLater(
          crdt.onTablesChanged.map((e) => e.tables), emits(['table_1']));
      await crdt.putAll({
        'table_1': {Uuid().v1(): 1},
        'table_2': {}
      });
    });

    test('Merge', () async {
      final crdt1 =
          await createCrdt({'table_1', 'table_2'}, sharedWith: '@bob');
      await crdt1.put('table_1', Uuid().v1(), 1);
      expectLater(
          crdt.onTablesChanged.map((e) => e.tables), emits(['table_1']));
      await crdt.merge(await crdt1.getChangeset());
    });
  });
}

class FakeAtClient extends Fake implements AtClient {
  final Map<AtKey, AtValue> data = {};

  @override
  Future<List<AtKey>> getAtKeys(
      {String? regex,
      String? sharedBy,
      String? sharedWith,
      bool showHiddenKeys = false}) {
    final keys = data.keys
        .where((k) => regex == null || RegExp(regex).hasMatch(k.toString()))
        .where((k) => k.sharedWith == sharedWith && k.sharedBy == sharedBy)
        .toList();
    print('FakeAtClient::getAtKeys: $regex, $sharedBy, $sharedWith');
    print('FakeAtClient::getAtKeys result: $keys');
    return Future.value(keys);
  }

  @override
  Future<bool> put(AtKey key, value,
      {bool isDedicated = false, PutRequestOptions? putRequestOptions}) {
    data[key] = AtValue()..value = value;
    print('FakeAtClient::put: $key, $value');
    return Future.value(true);
  }

  @override
  Future<AtValue> get(AtKey key,
      {bool isDedicated = false, GetRequestOptions? getRequestOptions}) {
    final value = data[key];
    if (value == null) {
      throw 'Key not found: $key in ${data.keys}';
    }
    return Future.value(value);
  }

  @override
  AtClientPreference? getPreferences() {
    return AtClientPreference()..namespace = 'testns';
  }

  @override
  String? getCurrentAtSign() {
    return '@alice';
  }
}
