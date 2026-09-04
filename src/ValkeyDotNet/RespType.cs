namespace ValkeyDotNet;

/// <summary>RESP2 and RESP3 value kinds.</summary>
public enum RespType
{
    Null,
    SimpleString,
    BlobString,
    VerbatimString,
    SimpleError,
    BlobError,
    Integer,
    Double,
    BigNumber,
    Boolean,
    Array,
    Map,
    Set,
    Push,
}
