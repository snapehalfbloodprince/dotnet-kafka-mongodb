namespace KafkaRouter.Worker.Parsing;

public interface IEventEnvelopeParser
{
    EventParseResult Parse(string? rawMessage);
}