using AMPSoft;

namespace Tests;

public class UnitTests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void TestHandlers()
    {
        var queue = new AzurePriorityPushQueue("UseDevelopmentStorage=true", "test");
        queue.Clear();
        TestContext.WriteLine("Queue created, but no handlers registered");

        queue.AddMessage("1");
        Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should be 1");

        Thread.Sleep(1000);

        Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should still be 1");

        queue.MessageReceived += MessageReceived;
        TestContext.WriteLine("Handler added");

        WaitForQueueLength(queue, 0);

        queue.MessageReceived -= MessageReceived;
        TestContext.WriteLine("Handler removed");

        queue.AddMessage("2");
        Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should be 1");

        Thread.Sleep(3000);
        Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should still be 1 since no handlers are added");
    }

    [Test]
    public void TestPriorities()
    {
        var queue = new AzurePriorityPushQueue("UseDevelopmentStorage=true", "test");
        queue.Clear();

        var expectedPriorities = new List<QueuePriority>
            {
                QueuePriority.High,
                QueuePriority.High,
                QueuePriority.Default,
                QueuePriority.Default,
                QueuePriority.Low,
                QueuePriority.Low,
            };

        foreach (var priority in expectedPriorities.Shuffle())
        {
            queue.AddMessage(priority.ToString(), priority);
        }

        Assert.AreEqual(expectedPriorities.Count, queue.ApproximateMessageCount(), $"ApproximateMessageCount should be {expectedPriorities.Count}");

        int expectedPriorityIndex = 0;
        queue.MessageReceived += (o, e) =>
        {
            Assert.AreEqual(e.MessageWrapper.Message.MessageText, expectedPriorities[expectedPriorityIndex++].ToString(), "Messages should be dequeued in prioritized order");
            e.MessageWrapper.Delete();
        };
        TestContext.WriteLine("Handler added");

        WaitForQueueLength(queue, 0);
    }

    [Test]
    public void TestMultipleMessages()
    {
        var queue = new AzurePriorityPushQueue("UseDevelopmentStorage=true", "test");
        queue.Clear();

        var expectedPriorities = new List<QueuePriority>
            {
                QueuePriority.High,
                QueuePriority.Default,
                QueuePriority.Low,
            };

        int messageCountPerQueue = 64;
        for (int i = 0; i < messageCountPerQueue; i++)
        {
            foreach (var priority in expectedPriorities)
            {
                queue.AddMessage(priority.ToString(), priority);
            }
        }

        Assert.AreEqual(expectedPriorities.Count * messageCountPerQueue, queue.ApproximateMessageCount(), $"ApproximateMessageCount should be {expectedPriorities.Count * messageCountPerQueue}");

        int expectedPriorityIndex = 0;
        queue.MessagesReceived += (o, e) =>
        {
            Assert.AreEqual(32, e.MessageWrappers.Count(), $"MessagesReceivedEventArgs should contain 32 messages");
            Parallel.ForEach(e.MessageWrappers, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, m =>
            {
                Assert.AreEqual(m.Message.MessageText, expectedPriorities[expectedPriorityIndex / 2].ToString(), "Messages should be dequeued in prioritized order");
                m.Delete();
            });
            expectedPriorityIndex++;
        };
        TestContext.WriteLine("Handler added");

        WaitForQueueLength(queue, 0);
    }

    void WaitForQueueLength(AzurePriorityPushQueue queue, int targetLength)
    {
        TestContext.WriteLine($"Waiting for queue length of {targetLength}");
        for (int i = 0; i < 60; i++)
        {
            Thread.Sleep(500);
            if (queue.ApproximateMessageCount() == targetLength)
            {
                Assert.AreEqual(targetLength, queue.ApproximateMessageCount(), $"ApproximateMessageCount should be {targetLength}");
                return;
            }
        }
        Assert.Fail($"Desired queue length of {targetLength} not reached after 30 seconds, current length {queue.ApproximateMessageCount()}");
    }

    void MessageReceived(object sender, MessageReceivedEventArgs e)
    {
        TestContext.WriteLine($"Received message: {e.MessageWrapper.Message.MessageText}, deleting");
        e.MessageWrapper.Delete();
    }
}