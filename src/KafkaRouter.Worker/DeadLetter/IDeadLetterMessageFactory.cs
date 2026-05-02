using Confluent.Kafka;
using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.DeadLetter;

public interface IDeadLetterMessageFactory
{
    string CreateDeadLetterPayload(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage,
        EventEnvelope? eventEnvelope = null);
}