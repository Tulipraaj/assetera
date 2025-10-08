import { executeQuery } from '@/lib/snowflake';
import { Customer } from '@/lib/types';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(
  request: NextRequest,
  context: { params: { id: string } }
) {
  const { id } = context.params;
  
  try {
    const query = `
      SELECT 
        c.CUSTOMER_ID,
        c.CONTACT_FIRST_NAME,
        c.CONTACT_LAST_NAME,
        c.STREET,
        c.CITY,
        c.STATE,
        c.ZIP,
        c.COUNTRY,
        c.AGE,
        c.MARITAL_STATUS,
        c.GENDER,
        c.NUMBER_OF_DEPENDENTS,
        v.FUND_ID,
        v.FINAL_RISK,
        v.TOTAL_ASSETS
      FROM CUSTOMERS c
      LEFT JOIN FUND_PREDICTIONS_FINAL v ON c.CUSTOMER_ID = v.CUSTOMER_ID
      WHERE c.CUSTOMER_ID = ?
    `;

    const [customer] = await executeQuery(query, [id]) as Customer[];

    if (!customer) {
      return NextResponse.json(
        { error: 'Customer not found' },
        { status: 404 }
      );
    }

    return NextResponse.json(customer);
  } catch (error) {
    console.error('Error fetching customer:', error);
    return NextResponse.json(
      { error: 'Failed to fetch customer' },
      { status: 500 }
    );
  }
}
