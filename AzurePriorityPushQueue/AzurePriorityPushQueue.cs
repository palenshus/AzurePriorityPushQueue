using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Polly;

namespace AMPSoft
{
    public enum QueuePriority
    {
        Low,
        Default,
        High
    }

    public class AzurePriorityPushQueue
    {
        Dictionary<QueuePriority, CloudQueue> queues;
        string connectionString;
        string queueNameBase;
        private EventHandler<MessageReceivedEventArgs> messageReceivedHandler;
        private EventHandler<MessagesReceivedEventArgs> messagesReceivedHandler;
        ManualResetEvent receivedHandled;
        CancellationTokenSource cancellationToken;

        public TimeSpan? VisibilityTimeout { get; set; }

        public int DequeueCount { get; set; } = 32;

        public AzurePriorityPushQueue(string connectionString, string queueName)
        {
            this.queues = new Dictionary<QueuePriority, CloudQueue>();
            this.connectionString = connectionString;
            this.queueNameBase = queueName;
            this.receivedHandled = new ManualResetEvent(false);
            this.cancellationToken = new CancellationTokenSource();
            Task.Run(() => ProcessMessages(), cancellationToken.Token);
        }

        ~AzurePriorityPushQueue()
        {
            cancellationToken.Cancel();
        }

        private void ProcessMessages()
        {
            int retryAttempt = 0;
            var policyFalse = Policy.HandleResult<bool>(false).WaitAndRetryForever((r) => TimeSpan.FromSeconds(Math.Min(8, Math.Pow(2, ++retryAttempt))));
            var policyTrue = Policy.HandleResult<bool>(true).RetryForever((result) => retryAttempt = 0);
            var policy = Policy.Wrap(policyFalse, policyTrue);

            policy.Execute((cancellationToken) =>
            {
                receivedHandled.WaitOne();
                foreach (QueuePriority priority in Enum.GetValues(typeof(QueuePriority)).Cast<QueuePriority>().OrderByDescending(p => p))
                {
                    var queue = GetAzureQueue(priority, false);
                    if (this.messageReceivedHandler != null)
                    {
                        var message = queue?.GetMessage(this.VisibilityTimeout);
                        if (message != null)
                        {
                            var eventArgs = new MessageReceivedEventArgs { MessageWrapper = new MessageWrapper(message, queue) };
                            OnMessageReceived(eventArgs);
                            return true;
                        }
                    }

                    if (this.messagesReceivedHandler != null)
                    {
                        var messages = queue?.GetMessages(DequeueCount, this.VisibilityTimeout);
                        if (messages.Any())
                        {
                            var eventArgs = new MessagesReceivedEventArgs { MessageWrappers = messages.Select(m => new MessageWrapper(m, queue)) };
                            OnMessagesReceived(eventArgs);
                            return true;
                        }
                    }
                }
                return false;
            }, this.cancellationToken.Token);
        }

        public void AddMessage<T>(T value, QueuePriority priority = QueuePriority.Default, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            string content = JsonConvert.SerializeObject(value);
            this.AddMessage(content, priority, timeToLive, initialVisibilityDelay);
        }

        public void AddMessage(string content, QueuePriority priority = QueuePriority.Default, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            this.AddMessage(new CloudQueueMessage(content), priority, timeToLive, initialVisibilityDelay);
        }

        public void AddMessage(CloudQueueMessage message, QueuePriority priority = QueuePriority.Default, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            GetAzureQueue(priority, true).AddMessage(message, timeToLive, initialVisibilityDelay);
        }

        public int? ApproximateMessageCount(QueuePriority priority)
        {
            var queue = GetAzureQueue(priority, false);
            queue?.FetchAttributes();
            return queue?.ApproximateMessageCount;
        }

        public int? ApproximateMessageCount()
        {
            return Enum.GetValues(typeof(QueuePriority)).Cast<QueuePriority>().Sum(p => this.ApproximateMessageCount(p));
        }

        public void Clear(QueuePriority priority)
        {
            GetAzureQueue(priority, false)?.Clear();
        }

        public void Clear()
        {
            Enum.GetValues(typeof(QueuePriority)).Cast<QueuePriority>().ToList().ForEach(p => this.Clear(p));
        }

        public event EventHandler<MessageReceivedEventArgs> MessageReceived
        {
            add
            {
                this.messageReceivedHandler += value;
                this.receivedHandled.Set();
            }
            remove
            {
                this.messageReceivedHandler -= value;
                if (this.messageReceivedHandler == null)
                {
                    this.receivedHandled.Reset();
                }
            }
        }

        public event EventHandler<MessagesReceivedEventArgs> MessagesReceived
        {
            add
            {
                this.messagesReceivedHandler += value;
                this.receivedHandled.Set();
            }
            remove
            {
                this.messagesReceivedHandler -= value;
                if (this.messagesReceivedHandler == null)
                {
                    this.receivedHandled.Reset();
                }
            }
        }

        protected virtual void OnMessageReceived(MessageReceivedEventArgs e)
        {
            this.messageReceivedHandler?.Invoke(this, e);
        }

        protected virtual void OnMessagesReceived(MessagesReceivedEventArgs e)
        {
            this.messagesReceivedHandler?.Invoke(this, e);
        }

        private CloudQueue GetAzureQueue(QueuePriority priority, bool createIfNotExists)
        {
            if (!queues.ContainsKey(priority))
            {
                // Retrieve storage account from connection string
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

                // Create the queue client
                CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

                string queueName = GetQueueName(this.queueNameBase, priority);

                // Retrieve a reference to a queue
                CloudQueue queue = queueClient.GetQueueReference(queueName);

                // Create the queue if it doesn't already exist and the flag is set
                if (createIfNotExists)
                {
                    queue.CreateIfNotExists();
                }

                if (queue.Exists())
                {
                    queues.Add(priority, queue);
                    return queue;
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return queues[priority];
            }
        }

        private string GetQueueName(string queueName, QueuePriority priority)
        {
            return $"{queueName}-{priority.ToString().ToLowerInvariant()}";
        }
    }

    public class MessageWrapper
    {
        CloudQueue queue;

        public CloudQueueMessage Message { get; set; }

        public MessageWrapper(CloudQueueMessage message, CloudQueue queue)
        {
            this.Message = message;
            this.queue = queue;
        }

        public void Delete()
        {
            this.queue.DeleteMessage(this.Message);
        }
    }

    public class MessageReceivedEventArgs : EventArgs
    {
        public MessageWrapper MessageWrapper { get; set; }
    }

    public class MessagesReceivedEventArgs : EventArgs
    {
        public IEnumerable<MessageWrapper> MessageWrappers { get; set; }
    }
}