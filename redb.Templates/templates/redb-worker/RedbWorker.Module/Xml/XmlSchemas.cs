namespace RedbWorker.Module.Xml;

/// <summary>The XSD schemas, compiled into the module as resources.</summary>
public static class XmlSchemas
{
    public static readonly string Order = Load("Order.xsd");

    private static string Load(string fileName)
    {
        var name = $"{typeof(XmlSchemas).Namespace}.{fileName}";
        using var stream = typeof(XmlSchemas).Assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"Embedded schema '{name}' was not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
