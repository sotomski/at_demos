import 'package:at_client/at_client.dart';
import 'package:mocktail/mocktail.dart';

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
        .where((k) =>
            k.sharedWith == sharedWith &&
            [sharedBy, currentAtSign].contains(k.sharedBy))
        .toList();
    print(
        'FakeAtClient($currentAtSign)::getAtKeys: $regex, $sharedBy, $sharedWith');
    print('FakeAtClient($currentAtSign)::getAtKeys result: $keys');
    return Future.value(keys);
  }

  @override
  Future<bool> put(AtKey key, value,
      {bool isDedicated = false, PutRequestOptions? putRequestOptions}) {
    data[key] = AtValue()..value = value;
    print('FakeAtClient($currentAtSign)::put: $key, $value');
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
  Future<bool> delete(AtKey key,
      {bool isDedicated = false, DeleteRequestOptions? deleteRequestOptions}) {
    data.remove(key);
    return Future.value(true);
  }

  @override
  AtClientPreference? getPreferences() {
    return AtClientPreference()..namespace = 'testns';
  }

  var currentAtSign = '@alice';

  @override
  String? getCurrentAtSign() {
    return currentAtSign;
  }
}
