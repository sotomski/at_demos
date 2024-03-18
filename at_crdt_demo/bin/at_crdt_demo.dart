import 'dart:async';
import 'dart:io';

import 'package:at_cli_commons/at_cli_commons.dart';
import 'package:at_crdt_demo/at_crdt.dart';
import 'package:chalkdart/chalkstrings.dart';

Future<void> main(List<String> arguments) async {
  try {
    final atClient = (await CLIBase.fromCommandLineArgs(arguments)).atClient;
    final crdts = <String, AtCrdt>{};

    stdout.writeln('Listening for commands...');
    stdout.writeln('Commands:');
    stdout.writeln('create <crdt> <tables>');
    stdout.writeln('delete <crdt>');
    stdout.writeln('merge <target_crdt> <source_crdt>');
    stdout.writeln('list <crdt> <table>');
    stdout.writeln('put <crdt> <table> <key> <value>');
    stdout.writeln('get <crdt> <table> <key>');
    stdout.writeln('q/quit/exit');
    stdout.writeln();

    while (true) {
      stdout.write('>>> ');
      final line = stdin.readLineSync();
      if (line == 'q' || line == 'quit' || line == 'exit') {
        break;
      }
      if (line == null) continue;

      // Parse input
      final [cmd, ...args] = line.split(' ');

      // Ensure a CRDT is available
      final crdtName = args[0];
      if (cmd != 'create' && !crdts.containsKey(crdtName)) {
        stdout.writeln(chalk.brightYellow(
            'No $crdtName CRDT available. Is it a typo or did you forget to \'create $crdtName <tables>\'?'));
      }

      // Execute command
      try {
        switch (cmd) {
          case 'create':
            final crdtName = args[0];
            final tables = args[1].split(',');
            crdts[crdtName] =
                AtCrdt(atClient: atClient, name: crdtName, tables: tables);
            await crdts[crdtName]!.init();
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
            final crdtName = args[0];
            final table = args[1];
            final map = await crdts[crdtName]?.getMap(table) ?? {};
            stdout.writeln(chalk.brightGreen('Content of $crdtName.$table:'));
            stdout.writeln(chalk.brightGreen('================='));
            for (final MapEntry(:key, :value) in map.entries) {
              stdout.writeln('$key: $value');
            }
            stdout.writeln(chalk.brightGreen('================='));
            break;
          case 'put':
            final crdtName = args[0];
            final table = args[1];
            final key = args[2];
            final value = args[3];
            await crdts[crdtName]?.put(table, key, value);
            break;
          case 'get':
            final crdtName = args[0];
            final table = args[1];
            final key = args[2];
            final value = await crdts[crdtName]?.get(table, key);
            stdout.writeln(chalk.brightGreen('$key: $value'));
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
