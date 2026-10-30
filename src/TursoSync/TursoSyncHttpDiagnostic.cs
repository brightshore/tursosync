namespace Turso.Sync;

/// <summary>The transport phase in which a request failed; HTTP error statuses alone are not transport failures.</summary>
public enum TursoSyncHttpFailurePhase
{
    None,
    BeforeHeaders,
    ResponseBody,
}

/// <summary>
/// One HTTP completion event. Contains no URL, query, headers, credentials, payload or exception text.
/// Method and endpoint are allowlisted labels; unknown values become OTHER and other respectively.
/// Timings exclude the diagnostics callback. Response bytes count bytes successfully read, including partial
/// reads before failure, not wire overhead. Null means that measurement was unavailable.
/// </summary>
/// <param name="Method">Standard HTTP method, or OTHER.</param>
/// <param name="Endpoint">info, pull-updates, push-updates, or other; never an arbitrary path.</param>
/// <param name="RequestBodyBytes">Request body length in bytes; zero for an absent or empty body.</param>
/// <param name="TimeToResponseHeaders">Time until headers arrived; null on a failure before headers.</param>
/// <param name="StatusCode">HTTP status, or null when headers did not arrive.</param>
/// <param name="DeclaredResponseBytes">Content-Length, or null when not declared or headers did not arrive.</param>
/// <param name="ActualResponseBytes">Bytes read from the body, or null if body reading never started.</param>
/// <param name="TotalDuration">Time spent sending and consuming the response, including cleanup.</param>
/// <param name="FailurePhase">None on transport completion, even for an HTTP error status.</param>
public sealed record TursoSyncHttpDiagnostic(
    string Method,
    string Endpoint,
    long RequestBodyBytes,
    TimeSpan? TimeToResponseHeaders,
    int? StatusCode,
    long? DeclaredResponseBytes,
    long? ActualResponseBytes,
    TimeSpan TotalDuration,
    TursoSyncHttpFailurePhase FailurePhase);
