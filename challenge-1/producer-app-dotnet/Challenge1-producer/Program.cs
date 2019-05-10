using Avro;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Confluent.SchemaRegistry;
using Dapper;
using Newtonsoft.Json;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Challenge1_producer
{
    public class BadgeEvent : ISpecificRecord
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string UserId { get; set; }
        public string DisplayName { get; set; }
        public string Reputation { get; set; }
        public int UpVotes { get; set; }
        public int DownVotes { get; set; }

        public Avro.Schema Schema => throw new NotImplementedException(@"GOT YOU SCHEMA!");

        public object Get(int fieldPos) => throw new NotImplementedException(@"GOT YOU GET!");
        public void Put(int fieldPos, object fieldValue) => throw new NotImplementedException(@"GOT YOU PUT!");
    }

    public class BadgeProducer
    {
        private readonly string _connectionString;
        private Logger _logger;
        private readonly string BrokerList;
        private readonly string TopicName;
        private readonly string SchemaRegistry;
        private CancellationTokenSource _cancellationTokenSource;
        private CancellationToken _cancellationToken;

        private volatile int _sleepTime = 1000; // msec

        public BadgeProducer(string _connectionString, string brokerList, string topic, string schemaRegistry)
        {
            this._connectionString = _connectionString;
            BrokerList = brokerList;
            TopicName = topic;
            SchemaRegistry = schemaRegistry;

            this._cancellationTokenSource = new CancellationTokenSource();
            this._cancellationToken = _cancellationTokenSource.Token;

            var config = new LoggingConfiguration();
            var target = new ConsoleTarget("console")
            {
                Layout = "${time} ${level:uppercase=true} ${message}"
            };
            config.AddTarget(target);
            var rule = new LoggingRule("*", LogLevel.Info, target);
            config.LoggingRules.Add(rule);
            LogManager.Configuration = config;

            _logger = LogManager.GetCurrentClassLogger();
        }

        public async Task Run()
        {
            _logger.Info("Testing connection to Azure SQL");
            try
            {
                var conn = new SqlConnection(_connectionString);
                conn.Open();
                conn.Close();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString(), $"Cannot connect to Azure SQL: {ex.Message}");
                return;
            }
            _logger.Info("Successfully connected to Azure SQL");

            var t = new Task(() => SendBadgeEvent(), TaskCreationOptions.LongRunning);
            t.Start();

            Console.ReadKey(true);
            _cancellationTokenSource.Cancel();

            await t;

        }

        private async Task<IEnumerable<BadgeEvent>> GetBadgeEvent()
        {
            _logger.Info($"Getting Badge events from database...");

            using (var connection = new SqlConnection(_connectionString))
            {
                var badgeEvents = await connection.QueryAsync<BadgeEvent>(
                    "Challenge1.GetBadge",
                    commandType: CommandType.StoredProcedure);

                _logger.Info($"Got {badgeEvents.Count()} badge event(s) from database.");

                return badgeEvents;
            };
        }

        private async void SendBadgeEvent()
        {

            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = SchemaRegistry }))
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = BrokerList
                };

                var avroSerializer = new Confluent.SchemaRegistry.Serdes.AvroSerializer<BadgeEvent>(schemaRegistry,
                    config: new Dictionary<string, string> { { "avro.serializer.auto.register.schemas", "false" } });

                using (var producer =
                    new ProducerBuilder<Null, BadgeEvent>(config)
                        .SetValueSerializer(avroSerializer)
                        .Build())
                {
                    while (!_cancellationToken.IsCancellationRequested)
                    {
                        var badgeEvent = await GetBadgeEvent();

                        foreach (var badge in badgeEvent)
                        {
                            _logger.Info("Sending Badge to Kafka...");

                            try
                            {
                                var deliveryReport = await producer.ProduceAsync(TopicName, new Message<Null, BadgeEvent> { Value = badge });

                                _logger.Info($"Badge delivered to: {deliveryReport.TopicPartitionOffset}");
                            }
                            catch (ProduceException<string, string> exception)
                            {
                                _logger.Error(exception, $"Failed to deliver message: {exception.Message} [{exception.Error.Code}]");
                                break;
                            }
                            catch (Exception exception)
                            {
                                _logger.Error(exception, $"Failed to deliver message: {exception.Message}");
                                break;
                            }
                        }

                        await Task.Delay(_sleepTime);
                    }
                }
            }
        }
    }

    public class SendEvent
    {
        private readonly string _brokerList;
        private readonly string _topic;

        public SendEvent(string brokerList, string topic)
        {
            _brokerList = brokerList;
            _topic = topic;
        }
    }

    public class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = ConfigurationManager.AppSettings["AZURE_SQL"];
                builder.UserID = ConfigurationManager.AppSettings["AZURE_SQL_USERNAME"];
                builder.Password = ConfigurationManager.AppSettings["AZURE_SQL_PASSWORD"];
                builder.InitialCatalog = ConfigurationManager.AppSettings["AZURE_SQL_DATABASE"];
                builder.MultipleActiveResultSets = false;
                builder.Encrypt = true;
                builder.TrustServerCertificate = false;
                builder.ConnectTimeout = 30;
                builder.PersistSecurityInfo = false;
                string connectionString = builder.ConnectionString;

                string brokerList = ConfigurationManager.AppSettings["KAFKA_BROKERS"];
                string topic = ConfigurationManager.AppSettings["KAFKA_TOPIC"];
                string schemaRegistry = ConfigurationManager.AppSettings["SCHEMA_REGISTRY"];

                BadgeProducer b = new BadgeProducer(connectionString, brokerList, topic, schemaRegistry);
                await b.Run();

            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}
