
class Aggregator:
    @staticmethod
    def aggregate_by_municipio(df):
        """
        Aggregates votes by municipality, cargo, and candidate.
        """
        # Group by
        group_cols = ['CD_MUNICIPIO', 'CD_CARGO_PERGUNTA', 'NR_VOTAVEL']
        
        # We need to sum QT_VOTOS
        # But we also need metadata like candidate name (NM_VOTAVEL), party (NR_PARTIDO)
        # Usually aggregation loses non-grouping columns unless we include them or use 'first'
        
        agg_funcs = {
            'QT_VOTOS': 'sum',
            'NM_VOTAVEL': 'first',
            'NR_PARTIDO': 'first',
            'SG_PARTIDO': 'first',
            'NM_PARTIDO': 'first',
            'DS_CARGO_PERGUNTA': 'first'
        }
        
        return df.groupby(group_cols, as_index=False).agg(agg_funcs)
