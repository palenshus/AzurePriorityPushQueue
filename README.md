# AzurePriorityPushQueue

An Azure Storage Queue wrapper that supports priorities and abstracts exponential backoff polling away with an event-driven interface

-- This is just an MVP at this stage, contributions welcome to get this fully featured!

# Installing via NuGet

    Install-Package AMPSoft.AzurePriorityPushQueue

# Usage

```csharp
var queue = new AzurePriorityPushQueue("<connection string>", "<queue-name>");

// Messages are added to Default queue by... default
queue.AddMessage("Default");

// Or you can specify a specific priority queue
queue.AddMessage("Low", QueuePriority.Low);
queue.AddMessage("Default", QueuePriority.Default);
queue.AddMessage("High", QueuePriority.High);

// Get the length of a specific queue
queue.ApproximateMessageCount(QueuePriority.Low); // returns 1

// Get the sum of all queue lengths
queue.ApproximateMessageCount(); // returns 3

// Register an event handler to handle dequeues
// Under the covers, it will poll with exponential backoff, up to a max of 30 seconds in between pollings
queue.Received += (o, e) =>
{
    Console.WriteLine(e.MessageWrapper.Message.AsString);
    e.MessageWrapper.Delete();
};