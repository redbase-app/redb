namespace redb.Examples.Output;

/// <summary>
/// Cell with optional color highlighting.
/// </summary>
public record TableCell(string Text, ConsoleColor? Color = null);

/// <summary>
/// Simple table printer with Unicode box-drawing and color support.
/// </summary>
public class TablePrinter
{
    private readonly int[] _widths;
    private readonly List<TableCell[]> _rows = [];
    private readonly List<int> _separatorAfter = []; // row indices after which to add separator

    public TablePrinter(params int[] columnWidths)
    {
        _widths = columnWidths;
    }

    /// <summary>
    /// Add header row.
    /// </summary>
    public TablePrinter Header(params string[] cells)
    {
        _rows.Add(cells.Select(c => new TableCell(c)).ToArray());
        _separatorAfter.Add(0); // separator after header
        return this;
    }

    /// <summary>
    /// Add data row with plain strings.
    /// </summary>
    public TablePrinter Row(params string[] cells)
    {
        _rows.Add(cells.Select(c => new TableCell(c)).ToArray());
        return this;
    }

    /// <summary>
    /// Add data row with colored cells.
    /// </summary>
    public TablePrinter Row(params TableCell[] cells)
    {
        _rows.Add(cells);
        return this;
    }

    /// <summary>
    /// Add row and mark separator after it.
    /// </summary>
    public TablePrinter RowWithSeparator(params string[] cells)
    {
        _rows.Add(cells.Select(c => new TableCell(c)).ToArray());
        _separatorAfter.Add(_rows.Count - 1);
        return this;
    }

    /// <summary>
    /// Add separator after last row.
    /// </summary>
    public TablePrinter Separator()
    {
        if (_rows.Count > 0)
            _separatorAfter.Add(_rows.Count - 1);
        return this;
    }

    /// <summary>
    /// Print table to console with colors.
    /// </summary>
    public void Print()
    {
        Console.WriteLine(TopLine());
        
        for (int i = 0; i < _rows.Count; i++)
        {
            PrintDataLine(_rows[i]);
            
            if (_separatorAfter.Contains(i) && i < _rows.Count - 1)
                Console.WriteLine(MidLine());
        }
        
        Console.WriteLine(BottomLine());
    }

    private string TopLine() => Line('┌', '┬', '┐');
    private string MidLine() => Line('├', '┼', '┤');
    private string BottomLine() => Line('└', '┴', '┘');

    private string Line(char left, char mid, char right)
    {
        var parts = _widths.Select(w => new string('─', w + 2));
        return $"{left}{string.Join(mid, parts)}{right}";
    }

    private void PrintDataLine(TableCell[] cells)
    {
        Console.Write('│');
        
        for (int i = 0; i < _widths.Length; i++)
        {
            var cell = i < cells.Length ? cells[i] : new TableCell("");
            var width = _widths[i];
            var text = cell.Text;
            
            // Truncate if needed
            if (text.Length > width)
                text = text[..(width - 2)] + "..";
            
            Console.Write(' ');
            
            // Apply color if specified
            if (cell.Color.HasValue)
            {
                var prev = Console.ForegroundColor;
                Console.ForegroundColor = cell.Color.Value;
                Console.Write(text.PadRight(width));
                Console.ForegroundColor = prev;
            }
            else
            {
                Console.Write(text.PadRight(width));
            }
            
            Console.Write(" │");
        }
        
        Console.WriteLine();
    }
}
