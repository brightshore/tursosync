using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

namespace Turso.Sync;

// Native callback signatures + value marshaling for user-defined functions, aggregates and collations.
// Ported from the official Turso.Data binding so the surface is at parity.

/// <summary>Scalar UDF native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoScalarFunctionCallback(IntPtr context, int argc, IntPtr argv, IntPtr contextDestructor, IntPtr valueDestructor);

/// <summary>Aggregate init native callback (allocates per-invocation state).</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate IntPtr TursoAggregateInitCallback(IntPtr context);

/// <summary>Aggregate step native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoAggregateStepCallback(IntPtr context, IntPtr aggregateContext, int argc, IntPtr argv);

/// <summary>Aggregate finalize native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate TursoExtensionValue TursoAggregateFinalCallback(IntPtr context, IntPtr aggregateContext);

/// <summary>Collation comparison native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate int TursoCollationCallback(IntPtr context, IntPtr leftPtr, UIntPtr leftLen, IntPtr rightPtr, UIntPtr rightLen);

/// <summary>Context destructor native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoContextDestructorCallback(IntPtr context);

/// <summary>Result-value destructor native callback.</summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void TursoValueDestructorCallback(IntPtr result);

/// <summary>Discriminator for <see cref="TursoExtensionValue"/>.</summary>
public enum TursoExtensionValueType
{
    /// <summary>NULL.</summary>
    Null = 0,

    /// <summary>64-bit integer.</summary>
    Integer = 1,

    /// <summary>Double.</summary>
    Float = 2,

    /// <summary>UTF-8 text.</summary>
    Text = 3,

    /// <summary>Binary blob.</summary>
    Blob = 4,

    /// <summary>Error result.</summary>
    Error = 5,
}

/// <summary>A tagged value passed to/from native UDFs (explicit layout matching the C ABI).</summary>
[StructLayout(LayoutKind.Explicit)]
public struct TursoExtensionValue
{
    /// <summary>The value discriminator.</summary>
    [FieldOffset(0)] public TursoExtensionValueType ValueType;

    /// <summary>The value payload.</summary>
    [FieldOffset(8)] public TursoExtensionValueUnion Value;
}

/// <summary>Union payload for <see cref="TursoExtensionValue"/>.</summary>
[StructLayout(LayoutKind.Explicit)]
public struct TursoExtensionValueUnion
{
    /// <summary>Integer payload.</summary>
    [FieldOffset(0)] public long IntValue;

    /// <summary>Real payload.</summary>
    [FieldOffset(0)] public double RealValue;

    /// <summary>Pointer to a text value.</summary>
    [FieldOffset(0)] public IntPtr TextValue;

    /// <summary>Pointer to a blob value.</summary>
    [FieldOffset(0)] public IntPtr BlobValue;

    /// <summary>Pointer to an error value.</summary>
    [FieldOffset(0)] public IntPtr ErrorValue;
}

/// <summary>
/// Marshaling for UDF/aggregate/collation callbacks: reads native argument values into CLR objects,
/// builds native result values from CLR objects, and owns the heap allocations until the engine's value
/// destructor frees them. Static callback instances are held alive for the process lifetime.
/// </summary>
internal static class TursoExtensionMarshal
{
    public static readonly TursoScalarFunctionCallback ScalarCallback = InvokeScalar;
    public static readonly TursoAggregateInitCallback AggregateInit = InitAggregate;
    public static readonly TursoAggregateStepCallback AggregateStep = StepAggregate;
    public static readonly TursoAggregateFinalCallback AggregateFinal = FinalizeAggregate;
    public static readonly TursoCollationCallback CollationCallback = InvokeCollation;
    public static readonly TursoContextDestructorCallback NoopContextDestructor = _ => { };
    public static readonly TursoContextDestructorCallback AggregateDestructor = DestroyAggregate;
    public static readonly TursoValueDestructorCallback ValueDestructor = DestroyValue;

    private static TursoExtensionValue InvokeScalar(IntPtr context, int argc, IntPtr argv, IntPtr contextDestructor, IntPtr valueDestructor)
    {
        try
        {
            var registration = (ScalarFunctionRegistration?)GCHandle.FromIntPtr(context).Target
                ?? throw new ObjectDisposedException(nameof(ScalarFunctionRegistration));
            var args = ReadArguments(argc, argv);
            object? result;
            using (ApplyGateHolds.Suspend())
            {
                result = registration.Invoke(args);
            }

            return CreateResult(result);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static IntPtr InitAggregate(IntPtr context)
    {
        var registration = (AggregateFunctionRegistration?)GCHandle.FromIntPtr(context).Target
            ?? throw new ObjectDisposedException(nameof(AggregateFunctionRegistration));
        return registration.CreateInvocationHandle();
    }

    private static TursoExtensionValue StepAggregate(IntPtr context, IntPtr aggregateContext, int argc, IntPtr argv)
    {
        try
        {
            var invocation = (AggregateInvocation?)GCHandle.FromIntPtr(aggregateContext).Target
                ?? throw new ObjectDisposedException(nameof(AggregateInvocation));
            var args = ReadArguments(argc, argv);
            using (ApplyGateHolds.Suspend())
            {
                invocation.Step(args);
            }

            return CreateResult(null);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static TursoExtensionValue FinalizeAggregate(IntPtr context, IntPtr aggregateContext)
    {
        try
        {
            var invocation = (AggregateInvocation?)GCHandle.FromIntPtr(aggregateContext).Target
                ?? throw new ObjectDisposedException(nameof(AggregateInvocation));
            object? result;
            using (ApplyGateHolds.Suspend())
            {
                result = invocation.FinalizeResult();
            }

            return CreateResult(result);
        }
        catch (Exception ex)
        {
            return CreateError(ex.Message);
        }
    }

    private static void DestroyAggregate(IntPtr aggregateContext)
    {
        if (aggregateContext == IntPtr.Zero)
        {
            return;
        }

        var handle = GCHandle.FromIntPtr(aggregateContext);
        if (handle.Target is AggregateInvocation invocation)
        {
            invocation.Registration.FreeInvocation(handle);
        }
        else if (handle.IsAllocated)
        {
            handle.Free();
        }
    }

    private static int InvokeCollation(IntPtr context, IntPtr leftPtr, UIntPtr leftLen, IntPtr rightPtr, UIntPtr rightLen)
    {
        var registration = (CollationRegistration?)GCHandle.FromIntPtr(context).Target
            ?? throw new ObjectDisposedException(nameof(CollationRegistration));
        var left = ReadUtf8(leftPtr, checked((int)leftLen));
        var right = ReadUtf8(rightPtr, checked((int)rightLen));
        using (ApplyGateHolds.Suspend())
        {
            return registration.Compare(left, right);
        }
    }

    private static void DestroyValue(IntPtr result)
    {
        if (result != IntPtr.Zero)
        {
            FreeExtensionValue(Marshal.PtrToStructure<TursoExtensionValue>(result));
        }
    }

    public static object?[] ReadArguments(int argc, IntPtr argv)
    {
        if (argc == 0)
        {
            return [];
        }

        var args = new object?[argc];
        var size = Marshal.SizeOf<TursoExtensionValue>();
        for (var i = 0; i < argc; i++)
        {
            var value = Marshal.PtrToStructure<TursoExtensionValue>(IntPtr.Add(argv, i * size));
            args[i] = value.ValueType switch
            {
                TursoExtensionValueType.Integer => value.Value.IntValue,
                TursoExtensionValueType.Float => value.Value.RealValue,
                TursoExtensionValueType.Text => ReadText(value.Value.TextValue),
                TursoExtensionValueType.Blob => ReadBlob(value.Value.BlobValue),
                _ => null,
            };
        }

        return args;
    }

    private static string ReadUtf8(IntPtr ptr, int length)
    {
        if (ptr == IntPtr.Zero || length == 0)
        {
            return string.Empty;
        }

        var bytes = new byte[length];
        Marshal.Copy(ptr, bytes, 0, bytes.Length);
        return Encoding.UTF8.GetString(bytes);
    }

    private static string ReadText(IntPtr textValuePtr)
    {
        if (textValuePtr == IntPtr.Zero)
        {
            return string.Empty;
        }

        var text = Marshal.PtrToStructure<ExtensionTextValue>(textValuePtr);
        if (text.Text == IntPtr.Zero || text.Length == 0)
        {
            return string.Empty;
        }

        var bytes = new byte[text.Length];
        Marshal.Copy(text.Text, bytes, 0, bytes.Length);
        return Encoding.UTF8.GetString(bytes);
    }

    private static byte[] ReadBlob(IntPtr blobValuePtr)
    {
        if (blobValuePtr == IntPtr.Zero)
        {
            return [];
        }

        var blob = Marshal.PtrToStructure<ExtensionBlobValue>(blobValuePtr);
        if (blob.Data == IntPtr.Zero || blob.Length == 0)
        {
            return [];
        }

        var bytes = new byte[checked((int)blob.Length)];
        Marshal.Copy(blob.Data, bytes, 0, bytes.Length);
        return bytes;
    }

    private static TursoExtensionValue CreateResult(object? value) => value switch
    {
        null or DBNull => new TursoExtensionValue { ValueType = TursoExtensionValueType.Null },
        bool b => Integer(b ? 1 : 0),
        byte v => Integer(v),
        sbyte v => Integer(v),
        short v => Integer(v),
        ushort v => Integer(v),
        int v => Integer(v),
        uint v => Integer(v),
        long v => Integer(v),
        float v => Real(v),
        double v => Real(v),
        decimal v => Real((double)v),
        byte[] v => Blob(v),
        _ => Text(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private static TursoExtensionValue Integer(long value) =>
        new() { ValueType = TursoExtensionValueType.Integer, Value = new TursoExtensionValueUnion { IntValue = value } };

    private static TursoExtensionValue Real(double value) =>
        new() { ValueType = TursoExtensionValueType.Float, Value = new TursoExtensionValueUnion { RealValue = value } };

    private static TursoExtensionValue Text(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var text = new ExtensionTextValue { Subtype = 0, Text = AllocBytes(bytes), Length = checked((uint)bytes.Length) };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionTextValue>());
        Marshal.StructureToPtr(text, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Text, Value = new TursoExtensionValueUnion { TextValue = ptr } };
    }

    private static TursoExtensionValue Blob(byte[] bytes)
    {
        var blob = new ExtensionBlobValue { Data = AllocBytes(bytes), Length = (ulong)bytes.Length };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionBlobValue>());
        Marshal.StructureToPtr(blob, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Blob, Value = new TursoExtensionValueUnion { BlobValue = ptr } };
    }

    private static TursoExtensionValue CreateError(string message)
    {
        var text = Text(message);
        var error = new ExtensionErrorValue { Code = 14, Message = text.Value.TextValue };
        var ptr = Marshal.AllocHGlobal(Marshal.SizeOf<ExtensionErrorValue>());
        Marshal.StructureToPtr(error, ptr, false);
        return new TursoExtensionValue { ValueType = TursoExtensionValueType.Error, Value = new TursoExtensionValueUnion { ErrorValue = ptr } };
    }

    private static IntPtr AllocBytes(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return IntPtr.Zero;
        }

        var data = Marshal.AllocHGlobal(bytes.Length);
        Marshal.Copy(bytes, 0, data, bytes.Length);
        return data;
    }

    private static void FreeExtensionValue(TursoExtensionValue value)
    {
        switch (value.ValueType)
        {
            case TursoExtensionValueType.Text:
                FreeText(value.Value.TextValue);
                break;
            case TursoExtensionValueType.Blob:
                FreeBlob(value.Value.BlobValue);
                break;
            case TursoExtensionValueType.Error:
                FreeError(value.Value.ErrorValue);
                break;
        }
    }

    private static void FreeText(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return;
        }

        var text = Marshal.PtrToStructure<ExtensionTextValue>(ptr);
        if (text.Text != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(text.Text);
        }

        Marshal.FreeHGlobal(ptr);
    }

    private static void FreeBlob(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return;
        }

        var blob = Marshal.PtrToStructure<ExtensionBlobValue>(ptr);
        if (blob.Data != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(blob.Data);
        }

        Marshal.FreeHGlobal(ptr);
    }

    private static void FreeError(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return;
        }

        var error = Marshal.PtrToStructure<ExtensionErrorValue>(ptr);
        FreeText(error.Message);
        Marshal.FreeHGlobal(ptr);
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionTextValue
    {
        public int Subtype;
        public IntPtr Text;
        public uint Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionBlobValue
    {
        public IntPtr Data;
        public ulong Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ExtensionErrorValue
    {
        public int Code;
        public IntPtr Message;
    }
}

/// <summary>A registered scalar function bound to a connection.</summary>
internal sealed class ScalarFunctionRegistration(string name, int argc, bool isDeterministic, Func<object?[], object?> invoke)
{
    public object? Invoke(object?[] args) => invoke(args);

    public GCHandle Register(IntPtr connection)
    {
        var handle = GCHandle.Alloc(this);
        try
        {
            var status = TursoNative.RegisterScalarFunction(
                connection, name, argc, isDeterministic, GCHandle.ToIntPtr(handle),
                TursoExtensionMarshal.ScalarCallback, TursoExtensionMarshal.NoopContextDestructor,
                TursoExtensionMarshal.ValueDestructor, out var errorPtr);
            TursoSyncDatabase.Check(status, errorPtr, "register_scalar_function");
            return handle;
        }
        catch
        {
            handle.Free();
            throw;
        }
    }
}

/// <summary>A registered aggregate function bound to a connection (manages per-invocation state handles).</summary>
internal sealed class AggregateFunctionRegistration(
    string name, int argc, object? seed, Func<object?, object?[], object?> step, Func<object?, object?> finalize)
{
    private readonly List<GCHandle> _invocations = [];

    public IntPtr CreateInvocationHandle()
    {
        var handle = GCHandle.Alloc(new AggregateInvocation(this, seed, step, finalize));
        lock (_invocations)
        {
            _invocations.Add(handle);
        }

        return GCHandle.ToIntPtr(handle);
    }

    public void FreeInvocation(GCHandle handle)
    {
        lock (_invocations)
        {
            _invocations.Remove(handle);
        }

        if (handle.IsAllocated)
        {
            handle.Free();
        }
    }

    public void FreeInvocations()
    {
        lock (_invocations)
        {
            foreach (var handle in _invocations)
            {
                if (handle.IsAllocated)
                {
                    handle.Free();
                }
            }

            _invocations.Clear();
        }
    }

    public GCHandle Register(IntPtr connection)
    {
        var handle = GCHandle.Alloc(this);
        try
        {
            var status = TursoNative.RegisterAggregateFunction(
                connection, name, argc, GCHandle.ToIntPtr(handle),
                TursoExtensionMarshal.AggregateInit, TursoExtensionMarshal.AggregateStep, TursoExtensionMarshal.AggregateFinal,
                TursoExtensionMarshal.NoopContextDestructor, TursoExtensionMarshal.AggregateDestructor,
                TursoExtensionMarshal.ValueDestructor, out var errorPtr);
            TursoSyncDatabase.Check(status, errorPtr, "register_aggregate_function");
            return handle;
        }
        catch
        {
            handle.Free();
            throw;
        }
    }
}

/// <summary>Per-aggregation accumulator state.</summary>
internal sealed class AggregateInvocation(
    AggregateFunctionRegistration registration, object? seed, Func<object?, object?[], object?> step, Func<object?, object?> finalize)
{
    private object? _accumulator = seed;

    public AggregateFunctionRegistration Registration { get; } = registration;

    public void Step(object?[] args) => _accumulator = step(_accumulator, args);

    public object? FinalizeResult() => finalize(_accumulator);
}

/// <summary>A registered collation bound to a connection.</summary>
internal sealed class CollationRegistration(string name, Func<string, string, int> compare)
{
    public int Compare(string left, string right) => compare(left, right);

    public GCHandle Register(IntPtr connection)
    {
        var handle = GCHandle.Alloc(this);
        try
        {
            var status = TursoNative.RegisterCollation(
                connection, name, GCHandle.ToIntPtr(handle),
                TursoExtensionMarshal.CollationCallback, TursoExtensionMarshal.NoopContextDestructor, out var errorPtr);
            TursoSyncDatabase.Check(status, errorPtr, "register_collation");
            return handle;
        }
        catch
        {
            handle.Free();
            throw;
        }
    }
}
