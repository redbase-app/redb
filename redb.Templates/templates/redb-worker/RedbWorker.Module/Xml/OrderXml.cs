using System.Xml.Serialization;

namespace RedbWorker.Module.Xml;

/// <summary>An inbound order, exactly as the file carries it. Checked against Order.xsd first.</summary>
[XmlRoot("Order")]
public sealed class OrderXml
{
    [XmlElement("OrderId")] public string OrderId { get; set; } = "";
    [XmlElement("Customer")] public string Customer { get; set; } = "";
    [XmlElement("Amount")] public decimal Amount { get; set; }
    [XmlElement("Currency")] public string Currency { get; set; } = "";
}

/// <summary>The answer written to the outbox for every file taken from the inbox.</summary>
[XmlRoot("OrderReceipt")]
public sealed class OrderReceiptXml
{
    [XmlElement("OrderId")] public string? OrderId { get; set; }
    [XmlElement("Status")] public string Status { get; set; } = "";
    [XmlElement("Reason")] public string? Reason { get; set; }
}

/// <summary>The values of <see cref="OrderReceiptXml.Status"/>.</summary>
public static class ReceiptStatuses
{
    public const string Accepted = "Accepted";
    public const string Duplicate = "Duplicate";
    public const string Rejected = "Rejected";
}
