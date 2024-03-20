import 'dart:async';
import 'dart:io';

import 'package:at_cli_commons/at_cli_commons.dart';
import 'package:at_client/at_client.dart';
import 'package:at_crdt_demo/at_crdt.dart';
import 'package:at_crdt_demo/at_crdt_sync.dart';
import 'package:chalkdart/chalkstrings.dart';

Future<void> main(List<String> arguments) async {
  try {
    final atClient = (await CLIBase.fromCommandLineArgs(arguments)).atClient;
    final crdts = <String, AtCrdt>{};

    // Sync with remote
    await syncWithRemote(atClient);

    _writeOutCommandInfo();

    while (true) {
      stdout.write('${atClient.getCurrentAtSign()}> ');
      final line = stdin.readLineSync();
      if (line == 'q' || line == 'quit' || line == 'exit') {
        break;
      }
      if (line == null) continue;

      // Parse input
      final [cmd, ...args] = line.split(' ');

      // Ensure a CRDT is available and initialized
      final crdtName = args.elementAtOrNull(0);
      if (!['init', 'help', 'syncremote'].contains(cmd) &&
          crdtName != null &&
          !crdts.containsKey(crdtName)) {
        stdout.writeln(chalk.brightYellow(
            'No $crdtName CRDT available. Is it a typo or did you forget to ${chalk.blueBright('init $crdtName <tables>')}?'));
      }

      // Execute command
      try {
        switch (cmd) {
          case 'init':
            final qualifiedName = args[0];
            final crdtSplit = qualifiedName.split(':');
            final otherSign = crdtSplit.length == 2 ? crdtSplit[0] : null;
            final crdtName =
                crdtSplit.length == 2 ? crdtSplit[1] : crdtSplit[0];
            final tables = args[1].split(',');
            if (otherSign != null) {
              stdout.writeln(chalk.brightBlue(
                  'Creating shared CRDT $crdtName with $otherSign'));
              crdts[qualifiedName] = AtCrdt.sharedWithOther(
                atClient: atClient,
                name: crdtName,
                otherAtSign: otherSign,
                tables: tables,
              );
            } else {
              stdout.writeln(
                  chalk.brightBlue('Creating self CRDT $qualifiedName'));
              crdts[qualifiedName] = AtCrdt.self(
                atClient: atClient,
                name: crdtName,
                tables: tables,
              );
            }
            stdout.writeln(chalk.brightBlue('Initializing $qualifiedName... '));
            await crdts[qualifiedName]!.init();
            stdout.writeln(chalk.brightGreen('OK'));
            break;
          case 'delete':
            final crdtName = args[0];
            await crdts[crdtName]?.delete();
            crdts.remove(crdtName);
            break;
          case 'merge':
            final targetCrdtName = args[0];
            final sourceCrdtName = args[1];
            await crdts[targetCrdtName]
                ?.merge(await crdts[sourceCrdtName]?.getChangeset() ?? {});
            break;
          case 'list':
            final qualifiedName = args[0];
            final table = args[1];
            final map = await crdts[qualifiedName]?.getMap(table) ?? {};
            stdout.writeln(
                chalk.brightGreen('Content of $qualifiedName.$table:'));
            for (final MapEntry(:key, :value) in map.entries) {
              stdout.writeln('$key: $value');
            }
            break;
          case 'put':
            final qualifiedName = args[0];
            final table = args[1];
            final key = args[2];
            final value = args[3];
            await crdts[qualifiedName]?.put(table, key, value);
            break;
          case 'get':
            final qualifiedName = args[0];
            final table = args[1];
            final key = args[2];
            final value = await crdts[qualifiedName]?.get(table, key);
            stdout.writeln(chalk.brightGreen('$key: $value'));
            break;
          case 'sync':
            final qualifiedName = args[0];
            final otherAtSign = args[1];
            final atSync = AtCrdtSync(
              atClient: atClient,
              crdtName: qualifiedName,
              tables: crdts[qualifiedName]!.tables,
            );
            await atSync.sync(otherAtSign: otherAtSign);
            break;
          case 'syncremote':
            await syncWithRemote(atClient);
            break;
          case 'help':
            _writeOutCommandInfo();
            break;
          default:
            stdout.writeln(chalk.red('Unknown command: $cmd'));
        }
      } catch (e, stack) {
        print('Error executing command: $cmd: ${chalk.red(e)}');
        print(chalk.red(stack));
      }
    }

    exit(0);
  } catch (e, stack) {
    print(e);
    print(stack);
    print(CLIBase.argsParser.usage);
  }
}

Future<void> syncWithRemote(AtClient atClient) async {
  // Wait for initial sync to complete
  stdout.write(chalk.brightBlue("Syncing your data."));
  var mySyncListener = MySyncProgressListener();
  atClient.syncService.addProgressListener(mySyncListener);
  atClient.syncService.sync();
  while (!mySyncListener.syncComplete) {
    await Future.delayed(Duration(milliseconds: 250));
    stdout.write(chalk.brightBlue('.'));
  }
  atClient.syncService.removeProgressListener(mySyncListener);
  stdout.writeln(chalk.brightGreen('OK'));
}

_writeOutCommandInfo() {
  stdout.writeln();
  stdout.writeln('Commands');
  stdout.writeln('=========');
  stdout.writeln();
  stdout.writeln('Single-CRDT commands');
  stdout.writeln('---------------------');
  stdout.writeln('Those commands allow addressing own and shared CRDTs');
  stdout.writeln(
    'It is possible to operate on own CRDTs and  shared with others',
  );
  stdout.writeln(
    '${chalk.blueBright('init <sharedWith?:crdt> <tables>')} => Example: init c1 t1; init @otherSign:c1 t1',
  );
  stdout.writeln(
    '${chalk.blueBright('delete <sharedWith?:crdt>')} => Example: delete c1; delete @otherSign:c1',
  );
  stdout.writeln(
      '${chalk.blueBright('list <sharedWith?:crdt> <table>')} => List the content of a table');
  stdout.writeln(
      '${chalk.blueBright('put <sharedWith?:crdt> <table> <key> <value>')} => Put a value into a table');
  stdout.writeln(
      '${chalk.blueBright('get <sharedWith?:crdt> <table> <key>')} => Get a value from a table');
  stdout.writeln(
      '${chalk.blueBright('merge <sharedWith?:target_crdt> <sharedWith?:source_crdt>')} => Merge the changeset of a source CRDT into a target CRDT');

  stdout.writeln();

  stdout.writeln('Multi-CRDT commands');
  stdout.writeln('---------------------');
  stdout.writeln('Those commands abstract away the sharing aspect');
  stdout.writeln(
      '${chalk.blueBright('sync <crdt> <otherAtSign>')} => Sync a CRDTs with another atSign');

  stdout.writeln();

  stdout.writeln('Other commands');
  stdout.writeln('---------------------');
  stdout.writeln('${chalk.blueBright('help')} => Shows this help');
  stdout
      .writeln('${chalk.blueBright('syncremote')} => Syncs with the atServer');
  stdout.writeln('${chalk.blueBright('q/quit/exit')} => Quits');
  stdout.writeln();

  stdout.writeln('Listening for commands...');
}
