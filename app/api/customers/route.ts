import { executeQuery } from '@/lib/snowflake';
import { CustomerWithFunds } from '@/lib/types';
import { NextResponse } from 'next/server';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get('page') || '1');
    const limit = parseInt(searchParams.get('limit') || '10');
    const search = searchParams.get('search') || '';
    const risk = searchParams.get('risk') || '';
    const fundId = searchParams.get('fundId') || '';
    const offset = (page - 1) * limit;

    // Only allow these 5 risk and 5 fund values for filtering
    const allowedRisks = ['1', '2', '3', '4', '5'];
    const allowedFunds = ['1', '2', '3', '4', '5'];
    const riskFilter = allowedRisks.includes(risk) ? risk : '';
    const fundIdFilter = allowedFunds.includes(fundId) ? fundId : '';

    let whereClauses = [
      "(CONCAT(c.CONTACT_FIRST_NAME, ' ', c.CONTACT_LAST_NAME) ILIKE '%' || ? || '%' OR CAST(c.CUSTOMER_ID AS STRING) = ?)"
    ];
    let params: any[] = [search, search];
    if (riskFilter) {
      whereClauses.push('v.FINAL_RISK = ?');
      params.push(Number(riskFilter));
    }
    if (fundIdFilter) {
      whereClauses.push('v.FUND_ID = ?');
      params.push(Number(fundIdFilter));
    }

    const whereSQL = whereClauses.length ? 'WHERE ' + whereClauses.join(' AND ') : '';

    const query = `
      SELECT 
        c.CUSTOMER_ID,
        c.CONTACT_FIRST_NAME,
        c.CONTACT_LAST_NAME,
        c.AGE,
        c.MARITAL_STATUS,
        c.GENDER,
        v.FUND_ID,
        v.FINAL_RISK,
        v.TOTAL_ASSETS
      FROM CUSTOMERS c
      LEFT JOIN FUND_PREDICTIONS_FINAL v ON c.CUSTOMER_ID = v.CUSTOMER_ID
      ${whereSQL}
      ORDER BY c.CUSTOMER_ID
      LIMIT ? OFFSET ?
    `;
    params.push(limit, offset);

    const customers = await executeQuery(query, params) as CustomerWithFunds[];

    const countQuery = `
      SELECT COUNT(*) as total
      FROM CUSTOMERS c
      LEFT JOIN FUND_PREDICTIONS_FINAL v ON c.CUSTOMER_ID = v.CUSTOMER_ID
      ${whereSQL}
    `;
    const countParams = params.slice(0, params.length - 2); // remove limit, offset
    const [{ total }] = await executeQuery(countQuery, countParams) as [{ total: number }];

    return NextResponse.json({
      customers,
      total,
      page,
      totalPages: Math.ceil(total / limit)
    });
  } catch (error) {
    console.error('Error fetching customers:', error);
    if (error instanceof Error) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      );
    }
    return NextResponse.json(
      { error: 'An unexpected error occurred while fetching customers' },
      { status: 500 }
    );
  }
}
