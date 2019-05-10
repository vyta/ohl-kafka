using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace T8.Consumer
{
    public static class EventHubTriggerCSharp
    {
        [FunctionName("EventHubTriggerCSharp")]
        public static async Task Run([EventHubTrigger("payment", Connection = "SourceEventHub")] EventData[] events, ILogger log, 
        [EventHub("outputPayments", Connection = "SourceEventHub")] IAsyncCollector<EventData> output)
        {
            var exceptions = new List<Exception>();
            
            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    var payment = JObject.Parse(messageBody);

                    payment["timeProcessed"] = DateTime.Now.ToString();
                    payment["paymentValid"] = "1";

                    await output.AddAsync(new EventData(Encoding.UTF8.GetBytes(payment.ToString())));
                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {payment}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();

        }
    }
}
