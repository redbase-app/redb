namespace redb.Core.Caching;

/// <summary>
/// Cache statistics.
/// </summary>
public class CacheStatistics
{
    public int SchemeHits { get; set; }
    public int SchemeMisses { get; set; }
    public int StructureHits { get; set; }
    public int StructureMisses { get; set; }
    public int TypeHits { get; set; }
    public int TypeMisses { get; set; }

    public double SchemeHitRatio => SchemeHits + SchemeMisses > 0 ? (double)SchemeHits / (SchemeHits + SchemeMisses) : 0;
    public double StructureHitRatio => StructureHits + StructureMisses > 0 ? (double)StructureHits / (StructureHits + StructureMisses) : 0;
    public double TypeHitRatio => TypeHits + TypeMisses > 0 ? (double)TypeHits / (TypeHits + TypeMisses) : 0;
    public double OverallHitRatio
    {
        get
        {
            var totalHits = SchemeHits + StructureHits + TypeHits;
            var totalRequests = SchemeHits + SchemeMisses + StructureHits + StructureMisses + TypeHits + TypeMisses;
            return totalRequests > 0 ? (double)totalHits / totalRequests : 0;
        }
    }
}
