import 'dart:async';

import 'package:at_client/at_client.dart';
import 'package:at_crdt_demo/at_crdt.dart';

class AtCrdtSync {
  final AtClient atClient;
  final String crdtName;
  final Set<String> tables;

  AtCrdtSync({
    required this.atClient,
    required this.crdtName,
    required this.tables,
  });

  // TODO: Should we always sync all tables? An API to discover tables would be necessary.
  // TODO: Notify other that it's time to sync.
  // TODO: Receive notification from other that it's time to sync.
  // TODO: Sync to multiple atSigns.
  // TODO: How to handle schema changes? Rejection? Versioning?
  /// Synchronizes the [crdt] with the [otherAtSign].
  /// The process involves:
  /// - If [importOthersChanges] is true, merging the CRDT shared by [otherAtSign] into the local CRDT
  /// - Merging the local CRDT with the CRDT shared with [otherAtSign]
  Future<bool> sync(
      {required String otherAtSign, bool importOthersChanges = true}) async {
    final local =
        AtCrdt.self(atClient: atClient, name: crdtName, tables: tables);
    final other = AtCrdt.sharedByOther(
        atClient: atClient,
        name: crdtName,
        tables: tables,
        otherAtSign: otherAtSign);
    final sharedWithOther = AtCrdt.sharedWithOther(
        atClient: atClient,
        name: crdtName,
        tables: tables,
        otherAtSign: otherAtSign);

    await Future.wait([local.init(), other.init(), sharedWithOther.init()]);

    await _logState(
        header: 'BEFORE SYNC',
        local: local,
        other: other,
        sharedWithOther: sharedWithOther);

    if (importOthersChanges) {
      final otherChanges = await other.getChangeset();
      await local.merge(otherChanges);
    }

    final localChanges = await local.getChangeset();
    await sharedWithOther.merge(localChanges);

    await _logState(
        header: 'AFTER SYNC',
        local: local,
        other: other,
        sharedWithOther: sharedWithOther);

    return true;
  }

  Future<void> _logState({
    required String header,
    required AtCrdt local,
    required AtCrdt other,
    required AtCrdt sharedWithOther,
  }) async {
    print('================================');
    print(header);
    print('================================');
    for (var table in tables) {
      print('Local: $table');
      (await local.getRecords(table)).forEach((key, value) {
        print('Local: $key => $value');
      });
    }
    print('================================');
    for (var table in tables) {
      print('Other: $table');
      (await other.getRecords(table)).forEach((key, value) {
        print('Other: $key => $value');
      });
    }
    print('================================');
    for (var table in tables) {
      print('Shared with other: $table');
      (await sharedWithOther.getRecords(table)).forEach((key, value) {
        print('Shared with other: $key => $value');
      });
    }
    print('================================');
  }
}

class FutureSyncProgressListener extends SyncProgressListener {
  final Completer<SyncProgress> _completer = Completer();

  Future<SyncProgress> get syncProgress => _completer.future;

  @override
  void onSyncProgressEvent(SyncProgress syncProgress) {
    if (syncProgress.syncStatus == SyncStatus.success ||
        syncProgress.syncStatus == SyncStatus.failure) {
      _completer.complete(syncProgress);
    }
  }
}
