namespace KafkaRouter.Worker.Processing;

public enum MessageProcessingOutcome
{
    ProcessedSuccessfully = 1,

    SentToDeadLetter = 2,

    SkippedAsDuplicate = 3
}