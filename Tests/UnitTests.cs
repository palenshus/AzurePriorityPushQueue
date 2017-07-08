using System.Collections.Generic;
using System.Threading;
using AMPSoft;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class UnitTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        public void TestHandlers()
        {
            var queue = new AzurePriorityPushQueue("UseDevelopmentStorage=true", "test");
            queue.Clear();
            TestContext.WriteLine("Queue created, but no handlers registered");
            
            queue.AddMessage("1");
            Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should be 1");

            Thread.Sleep(1000);

            Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should still be 1");

            queue.Received += MessageReceived;
            TestContext.WriteLine("Handler added");
            Thread.Sleep(2000);

            Assert.AreEqual(0, queue.ApproximateMessageCount(), "Now queue length should be 0");

            queue.Received -= MessageReceived;
            TestContext.WriteLine("Handler removed");

            queue.AddMessage("2");
            Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should be 1");

            Thread.Sleep(3000);
            Assert.AreEqual(1, queue.ApproximateMessageCount(), "ApproximateMessageCount should still be 1 since no handlers are added");
        }

        [TestMethod]
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

            foreach (var priority in expectedPriorities)
            {
                queue.AddMessage(priority.ToString(), priority);
            }

            Assert.AreEqual(6, queue.ApproximateMessageCount(), $"ApproximateMessageCount should be {expectedPriorities.Count}");

            int expectedPriorityIndex = 0;
            queue.Received += (o, e) =>
            {
                Assert.AreEqual(e.MessageWrapper.Message.AsString, expectedPriorities[expectedPriorityIndex++].ToString(), "Messages should be dequeued in prioritized order");
                e.MessageWrapper.Delete();
            };
            TestContext.WriteLine("Handler added");

            Thread.Sleep(2000);
            Assert.AreEqual(0, queue.ApproximateMessageCount(), "ApproximateMessageCount should be 0");
        }

        void MessageReceived(object sender, MessageReceivedEventArgs e)
        {
            TestContext.WriteLine($"Received message: {e.MessageWrapper.Message.AsString}, deleting");
            e.MessageWrapper.Delete();
        }
    }
}
