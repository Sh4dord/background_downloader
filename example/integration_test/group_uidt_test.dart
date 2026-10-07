// ignore_for_file: avoid_print

import 'dart:async';
import 'dart:io';

import 'package:background_downloader/background_downloader.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_utils.dart';
import 'uidt_test.dart' show listenToTasks;

const groupUIDTGroup = 'groupUIDT';

void main() {
  setUp(groupUIDTSetup);
  tearDown(groupUIDTTearDown);

  group('Group UIDT', () {
    testWidgets(
      'Group of downloads completes',
      timeout: const Timeout(Duration(minutes: 3)),
      (tester) async {
        final tasks = List<Task>.generate(
          5,
          (i) => DownloadTask(
            url: urlWithContentLength,
            filename: 'group_uidt_$i.zip',
            group: groupUIDTGroup,
            updates: Updates.status,
            priority: 0,
          ),
        );
        final completers = {
          for (final task in tasks)
            task: {TaskStatus.complete: Completer<void>()},
        };
        listenToTasks(tasks, statusCompleters: completers);

        for (final task in tasks) {
          expect(await FileDownloader().enqueue(task), isTrue);
        }
        await Future.wait(
          completers.values.map((c) => c[TaskStatus.complete]!.future),
        );
        for (final task in tasks) {
          final file = File(await task.filePath());
          expect(file.existsSync(), isTrue);
          expect(file.lengthSync(), equals(urlWithContentLengthFileSize));
        }
        expect(await FileDownloader().allTasks(group: groupUIDTGroup), isEmpty);
      },
    );

    testWidgets(
      'Task added while the job runs is picked up',
      timeout: const Timeout(Duration(minutes: 3)),
      (tester) async {
        final longTask = DownloadTask(
          url: urlWithLongContentLength,
          filename: 'group_uidt_long.bin',
          group: groupUIDTGroup,
          updates: Updates.status,
          priority: 0,
        );
        final shortTask = DownloadTask(
          url: urlWithContentLength,
          filename: 'group_uidt_short.zip',
          group: groupUIDTGroup,
          updates: Updates.status,
          priority: 0,
        );
        final longRunning = Completer<void>();
        final longCanceled = Completer<void>();
        final shortComplete = Completer<void>();
        listenToTasks(
          <Task>[longTask, shortTask],
          statusCompleters: {
            longTask: {
              TaskStatus.running: longRunning,
              TaskStatus.canceled: longCanceled,
            },
            shortTask: {TaskStatus.complete: shortComplete},
          },
        );

        expect(await FileDownloader().enqueue(longTask), isTrue);
        await longRunning.future;
        expect(await FileDownloader().enqueue(shortTask), isTrue);
        await shortComplete.future;
        expect(File(await shortTask.filePath()).existsSync(), isTrue);

        expect(
          await FileDownloader().cancelTasksWithIds([longTask.taskId]),
          isTrue,
        );
        await longCanceled.future;
      },
    );

    testWidgets(
      'allTasks lists group tasks and cancel stops them',
      timeout: const Timeout(Duration(minutes: 3)),
      (tester) async {
        final tasks = List<Task>.generate(
          3,
          (i) => DownloadTask(
            url: urlWithLongContentLength,
            filename: 'group_uidt_cancel_$i.bin',
            group: groupUIDTGroup,
            updates: Updates.status,
            priority: 0,
          ),
        );
        final completers = {
          for (final task in tasks)
            task: {
              TaskStatus.running: Completer<void>(),
              TaskStatus.canceled: Completer<void>(),
            },
        };
        listenToTasks(tasks, statusCompleters: completers);

        for (final task in tasks) {
          expect(await FileDownloader().enqueue(task), isTrue);
        }
        await Future.wait(
          completers.values.map((c) => c[TaskStatus.running]!.future),
        );
        final allTasks = await FileDownloader().allTasks(group: groupUIDTGroup);
        expect(
          allTasks.map((t) => t.taskId).toSet(),
          equals(tasks.map((t) => t.taskId).toSet()),
        );
        expect(await FileDownloader().taskForId(tasks.first.taskId), isNotNull);

        // cancel one, then the rest
        expect(
          await FileDownloader().cancelTasksWithIds([tasks.first.taskId]),
          isTrue,
        );
        await completers[tasks.first]![TaskStatus.canceled]!.future;
        expect(
          await FileDownloader().allTasks(group: groupUIDTGroup),
          hasLength(2),
        );
        expect(await FileDownloader().cancelAll(group: groupUIDTGroup), isTrue);
        await Future.wait(
          completers.values.map((c) => c[TaskStatus.canceled]!.future),
        );
        expect(await FileDownloader().allTasks(group: groupUIDTGroup), isEmpty);
      },
    );

    testWidgets(
      'Group of downloads completes with a holding queue',
      timeout: const Timeout(Duration(minutes: 3)),
      (tester) async {
        await FileDownloader().configure(
          globalConfig: [(Config.holdingQueue, (2, null, null))],
        );
        final tasks = List<Task>.generate(
          6,
          (i) => DownloadTask(
            url: urlWithContentLength,
            filename: 'group_uidt_hq_$i.zip',
            group: groupUIDTGroup,
            updates: Updates.status,
            priority: 0,
          ),
        );
        final completers = {
          for (final task in tasks)
            task: {TaskStatus.complete: Completer<void>()},
        };
        listenToTasks(tasks, statusCompleters: completers);

        for (final task in tasks) {
          expect(await FileDownloader().enqueue(task), isTrue);
        }
        await Future.wait(
          completers.values.map((c) => c[TaskStatus.complete]!.future),
        );
        for (final task in tasks) {
          expect(File(await task.filePath()).existsSync(), isTrue);
        }
      },
    );

    testWidgets(
      'Uploads and downloads share the group job',
      timeout: const Timeout(Duration(minutes: 3)),
      (tester) async {
        final download = DownloadTask(
          url: urlWithContentLength,
          filename: 'group_uidt_mixed.zip',
          group: groupUIDTGroup,
          updates: Updates.status,
          priority: 0,
        );
        final upload = UploadTask(
          url: uploadTestUrl,
          filename: uploadFilename,
          group: groupUIDTGroup,
          updates: Updates.status,
          priority: 0,
        );
        final completers = <Task, Map<TaskStatus, Completer<void>>>{
          download: {TaskStatus.complete: Completer<void>()},
          upload: {TaskStatus.complete: Completer<void>()},
        };
        listenToTasks(<Task>[download, upload], statusCompleters: completers);

        expect(await FileDownloader().enqueue(download), isTrue);
        expect(await FileDownloader().enqueue(upload), isTrue);
        await Future.wait(
          completers.values.map((c) => c[TaskStatus.complete]!.future),
        );
      },
    );
  });
}

Future<void> groupUIDTSetup() async {
  await defaultSetup();
  await FileDownloader().reset(group: groupUIDTGroup);
  await FileDownloader().configure(androidConfig: [(Config.groupUIDT, true)]);
  FileDownloader().configureNotificationForGroup(
    groupUIDTGroup,
    running: const TaskNotification(
      'Group running',
      '{numFinished} of {numTotal}',
    ),
    complete: const TaskNotification('Group complete', '{numTotal} files'),
    error: const TaskNotification('Group error', '{numFailed} failed'),
    progressBar: true,
    groupNotificationId: groupUIDTGroup,
  );
}

Future<void> groupUIDTTearDown() async {
  await FileDownloader().reset(group: groupUIDTGroup);
  await FileDownloader().configure(androidConfig: [(Config.groupUIDT, false)]);
  await defaultTearDown();
}
