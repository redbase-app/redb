Scatter-gather without a broker, `routes/quotes.route.xml`: a timer sends a price request to three
`direct://` supplier routes at once with `<scatterGather parallel="true" timeout="00:00:01"
strategy="min:${jpath('$.price')}">`; supplier C is slower than the timeout and is left out.

Usually changed: the recipients become real endpoints (HTTP, request-reply over a queue, a `sql:` query);
the strategy (`groupedBody` keeps every answer, `max:`, `useLatest`, `concat:`); the timeout.

`routes/QuoteRoute.cs` sends a second request in C# on its own timer (`bulk.period`) to the same three supplier
routes of the XML file (it only produces to their `direct:` names) and folds the answers with a C# function that
keeps the whole cheapest answer.
