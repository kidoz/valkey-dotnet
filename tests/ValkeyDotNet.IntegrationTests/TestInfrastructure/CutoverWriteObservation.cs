namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal static class CutoverWriteObservation
{
    internal static async Task<string> ObserveAsync(Task<RespValue> write)
    {
        await ((Task)write).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        if (write.IsCompletedSuccessfully)
        {
            var reply = await write;
            return reply.Type == RespType.SimpleString && reply.AsString() == "OK"
                ? "acknowledged"
                : "unexpected_reply";
        }
        if (write.IsCanceled)
        {
            try
            {
                await write;
            }
            catch (OperationCanceledException cancelled) when (cancelled is IValkeyCommandFailure cancelledFailure)
            {
                return Classify(cancelledFailure.DeliveryStatus);
            }
            catch (OperationCanceledException)
            {
                return "cancelled_without_delivery_status";
            }
            return "cancelled_without_delivery_status";
        }
        var exception = write.Exception!.InnerException!;
        if (exception is IValkeyCommandFailure failure)
        {
            return Classify(failure.DeliveryStatus);
        }
        if (exception is OperationCanceledException)
        {
            return "cancelled_without_delivery_status";
        }
        return "unexpected_failure";
    }

    private static string Classify(ValkeyCommandDeliveryStatus status) =>
        status switch
        {
            ValkeyCommandDeliveryStatus.NotSent => "not_sent",
            ValkeyCommandDeliveryStatus.MayHaveBeenSent => "ambiguous",
            ValkeyCommandDeliveryStatus.ReplyReceived => "reply_error",
            _ => "unexpected_failure",
        };
}
