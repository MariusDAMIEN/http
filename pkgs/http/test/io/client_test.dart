// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:http/io_client.dart' as http_io;
import 'package:test/test.dart';

import '../utils.dart';

class TestClient extends http.BaseClient {
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    throw UnimplementedError();
  }
}

class TestClient2 extends http.BaseClient {
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    throw UnimplementedError();
  }
}

void main() {
  late Uri serverUrl;
  setUpAll(() async {
    serverUrl = await startServer();
  });

  test('#send a StreamedRequest', () async {
    var client = http.Client();
    var request = http.StreamedRequest('POST', serverUrl)
      ..headers[HttpHeaders.contentTypeHeader] =
          'application/json; charset=utf-8'
      ..headers[HttpHeaders.userAgentHeader] = 'Dart';

    var responseFuture = client.send(request);
    request.sink.add('{"hello": "world"}'.codeUnits);
    unawaited(request.sink.close());

    var response = await responseFuture;

    expect(response.request, equals(request));
    expect(response.statusCode, equals(200));
    expect(response.headers['single'], equals('value'));
    // dart:io internally normalizes outgoing headers so that they never
    // have multiple headers with the same name, so there's no way to test
    // whether we handle that case correctly.

    var bytesString = await response.stream.bytesToString();
    client.close();
    expect(
        bytesString,
        parse(equals({
          'method': 'POST',
          'path': '/',
          'headers': {
            'content-type': ['application/json; charset=utf-8'],
            'accept-encoding': ['gzip'],
            'user-agent': ['Dart'],
            'transfer-encoding': ['chunked']
          },
          'body': '{"hello": "world"}'
        })));
  });

  test('#send a StreamedRequest with a custom client', () async {
    var ioClient = HttpClient();
    var client = http_io.IOClient(ioClient);
    var request = http.StreamedRequest('POST', serverUrl)
      ..headers[HttpHeaders.contentTypeHeader] =
          'application/json; charset=utf-8'
      ..headers[HttpHeaders.userAgentHeader] = 'Dart';

    var responseFuture = client.send(request);
    request.sink.add('{"hello": "world"}'.codeUnits);
    unawaited(request.sink.close());

    var response = await responseFuture;

    expect(response.request, equals(request));
    expect(response.statusCode, equals(200));
    expect(response.headers['single'], equals('value'));
    // dart:io internally normalizes outgoing headers so that they never
    // have multiple headers with the same name, so there's no way to test
    // whether we handle that case correctly.

    var bytesString = await response.stream.bytesToString();
    client.close();
    expect(
        bytesString,
        parse(equals({
          'method': 'POST',
          'path': '/',
          'headers': {
            'content-type': ['application/json; charset=utf-8'],
            'accept-encoding': ['gzip'],
            'user-agent': ['Dart'],
            'transfer-encoding': ['chunked']
          },
          'body': '{"hello": "world"}'
        })));
  });

  test('#send with an invalid URL', () {
    var client = http.Client();
    var url = Uri.http('http.invalid', '');
    var request = http.StreamedRequest('POST', url);
    request.headers[HttpHeaders.contentTypeHeader] =
        'application/json; charset=utf-8';

    expect(
        client.send(request),
        throwsA(allOf(
            isA<http.ClientException>().having((e) => e.uri, 'uri', url),
            isA<SocketException>().having(
                (e) => e.toString(),
                'SocketException.toString',
                matches('ClientException with SocketException.*,'
                    ' uri=http://http.invalid')))));

    request.sink.add('{"hello": "world"}'.codeUnits);
    request.sink.close();
  });

  test('sends a MultipartRequest with correct content-type header', () async {
    var client = http.Client();
    var request = http.MultipartRequest('POST', serverUrl);

    var response = await client.send(request);

    var bytesString = await response.stream.bytesToString();
    client.close();

    var headers = (jsonDecode(bytesString) as Map<String, dynamic>)['headers']
        as Map<String, dynamic>;
    var contentType = (headers['content-type'] as List).single;
    expect(contentType, startsWith('multipart/form-data; boundary='));
  });

  test('detachSocket returns a socket from an IOStreamedResponse', () async {
    var ioClient = HttpClient();
    var client = http_io.IOClient(ioClient);
    var request = http.Request('GET', serverUrl);

    var response = await client.send(request);
    var socket = await response.detachSocket();

    expect(socket, isNotNull);
  });

  test('runWithClient', () {
    final client = http.runWithClient(http.Client.new, TestClient.new);
    expect(client, isA<TestClient>());
  });

  test('runWithClient Client() return', () {
    final client = http.runWithClient(http.Client.new, http.Client.new);
    expect(client, isA<http_io.IOClient>());
  });

  test('runWithClient nested', () {
    late final http.Client client;
    late final http.Client nestedClient;
    http.runWithClient(() {
      http.runWithClient(() => nestedClient = http.Client(), TestClient2.new);
      client = http.Client();
    }, TestClient.new);
    expect(client, isA<TestClient>());
    expect(nestedClient, isA<TestClient2>());
  });

  test('runWithClient recursion', () {
    // Verify that calling the http.Client() factory inside nested Zones does
    // not provoke an infinite recursion.
    http.runWithClient(() {
      http.runWithClient(http.Client.new, http.Client.new);
    }, http.Client.new);
  });

  group('307/308 method-preserving redirects', () {
    test('POST + 307 preserves method and body', () async {
      final client = http_io.IOClient();
      final request = http.Request('POST', serverUrl.resolve('/redirect307'))
        ..headers[HttpHeaders.contentTypeHeader] = 'application/json'
        ..body = '{"hello":"world"}';

      final response = await client.send(request);
      final decoded = jsonDecode(await response.stream.bytesToString())
          as Map<String, dynamic>;
      client.close();

      expect(decoded['method'], equals('POST'));
      expect(decoded['path'], equals('/'));
      expect(decoded['body'], equals('{"hello":"world"}'));
    });

    test('POST + 308 preserves method and body', () async {
      final client = http_io.IOClient();
      final request = http.Request('POST', serverUrl.resolve('/redirect308'))
        ..headers[HttpHeaders.contentTypeHeader] = 'application/json'
        ..body = '{"hello":"world"}';

      final response = await client.send(request);
      final decoded = jsonDecode(await response.stream.bytesToString())
          as Map<String, dynamic>;
      client.close();

      expect(decoded['method'], equals('POST'));
      expect(decoded['path'], equals('/'));
      expect(decoded['body'], equals('{"hello":"world"}'));
    });

    test('PUT + 307 preserves method and body', () async {
      final client = http_io.IOClient();
      final request = http.Request('PUT', serverUrl.resolve('/redirect307'))
        ..headers[HttpHeaders.contentTypeHeader] = 'application/json'
        ..body = '{"updated":true}';

      final response = await client.send(request);
      final decoded = jsonDecode(await response.stream.bytesToString())
          as Map<String, dynamic>;
      client.close();

      expect(decoded['method'], equals('PUT'));
      expect(decoded['path'], equals('/'));
      expect(decoded['body'], equals('{"updated":true}'));
    });

    test('exceeding maxRedirects on 307 loop throws ClientException', () async {
      final client = http_io.IOClient();
      final request =
          http.Request('POST', serverUrl.resolve('/loop307?n=0'))
            ..maxRedirects = 2
            ..body = 'data';

      await expectLater(
        client.send(request),
        throwsA(isA<http.ClientException>()),
      );
      client.close();
    });

    test('GET + 307 still delegates redirect to dart:io (no manual loop)',
        () async {
      final client = http_io.IOClient();
      // A GET request should follow the 307 without manual intervention and
      // land on '/' with an empty body (GET has no body to replay).
      final request = http.Request('GET', serverUrl.resolve('/redirect307'));

      final response = await client.send(request);
      final decoded = jsonDecode(await response.stream.bytesToString())
          as Map<String, dynamic>;
      client.close();

      expect(decoded['method'], equals('GET'));
      expect(decoded['path'], equals('/'));
    });

    test(
        'StreamedRequest + 307 is not followed manually (body not replayable)',
        () async {
      final client = http_io.IOClient();
      final request =
          http.StreamedRequest('POST', serverUrl.resolve('/redirect307'))
            ..headers[HttpHeaders.contentTypeHeader] = 'application/json'
            // Disable auto-follow so the raw 307 is returned to the caller.
            ..followRedirects = false;

      final responseFuture = client.send(request);
      request.sink.add('{"hello":"world"}'.codeUnits);
      unawaited(request.sink.close());

      final response = await responseFuture;
      client.close();

      // The 307 should be returned as-is — no manual replay occurred.
      expect(response.statusCode, equals(307));
    });
  });
}
