'use client';

import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';

export function BarChartHorizontal({ data, height = 400 }: { data: any[], height?: number }) {
  const total = data.reduce((acc, curr) => acc + curr.eleitores, 0);
  
  const plotData = data.map(d => ({
    ...d,
    pct: total > 0 ? (d.eleitores / total) * 100 : 0
  }));

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const { label, eleitores, pct } = payload[0].payload;
      return (
        <div className="bg-white p-2 border shadow-sm text-sm">
          <p className="font-bold">{label}</p>
          <p>{new Intl.NumberFormat('pt-BR').format(eleitores)} eleitores ({pct.toFixed(2)}%)</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div style={{ width: '100%', height }}>
      <ResponsiveContainer>
        <BarChart
          layout="vertical"
          data={plotData}
          margin={{ top: 10, right: 30, left: 0, bottom: 10 }}
        >
          <XAxis type="number" hide />
          <YAxis 
            dataKey="label" 
            type="category" 
            axisLine={false} 
            tickLine={false}
            width={150}
            tick={{ fontSize: 11, fill: '#5b6b80' }}
          />
          <Tooltip content={<CustomTooltip />} cursor={{fill: 'transparent'}} />
          <Bar dataKey="pct" fill="#1f6feb" radius={[0, 4, 4, 0]}>
            {plotData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill="#1f6feb" />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
