using System.Globalization;
using redb.Route.Abstractions;

namespace RouteModule.Beans;

/// <summary>
/// A bean of the module: logic that does not belong in markup. context.xml builds one instance
/// from configuration (constructor argument and property); the routes call its methods through
/// <c>bean:#pricing?method=...</c>.
/// </summary>
public sealed class PriceCalculator
{
    /// <summary>Creates the calculator with the VAT rate, for example 0.20.</summary>
    public PriceCalculator(decimal vatRate)
    {
        if (vatRate < 0)
            throw new ArgumentOutOfRangeException(nameof(vatRate), "The VAT rate cannot be negative.");
        VatRate = vatRate;
    }

    /// <summary>The VAT rate the gross prices include.</summary>
    public decimal VatRate { get; }

    /// <summary>The currency written on every order; set from configuration.</summary>
    public string Currency { get; set; } = "EUR";

    /// <summary>
    /// The gross amount of an order line. Called as <c>method=Gross(${header.qty}, ${header.price})</c>:
    /// each argument is an expression, converted to the parameter type; the result becomes the body.
    /// </summary>
    public decimal Gross(int qty, decimal price)
    {
        if (qty <= 0)
            throw new ArgumentOutOfRangeException(nameof(qty), "The quantity must be positive.");
        return Math.Round(qty * price * (1 + VatRate), 2, MidpointRounding.AwayFromZero);
    }

    /// <summary>
    /// Writes the currency and the VAT rate into the headers. Called as <c>method=Stamp</c>: a method
    /// that takes the exchange reads and changes it directly; a void method leaves the body alone.
    /// </summary>
    public void Stamp(IExchange exchange)
    {
        exchange.In.Headers["currency"] = Currency;
        exchange.In.Headers["vat"] = VatRate.ToString(CultureInfo.InvariantCulture);
    }
}
