import { executeQuery } from '@/lib/snowflake';
import { CustomerUpdatePayload } from '@/lib/types';
import { NextResponse } from 'next/server';

export async function PUT(request: Request) {
  try {
    const body = await request.json() as CustomerUpdatePayload;
    
    const { CUSTOMER_ID, FINAL_RISK, FUND_ID } = body;

    // Update fund prediction
    const updateFundQuery = `
      UPDATE FUND_PREDICTIONS_FINAL
      SET FUND_ID = ?,
          FINAL_RISK = ?
      WHERE CUSTOMER_ID = ?
    `;
    
    await executeQuery(updateFundQuery, [body.FUND_ID, body.FINAL_RISK, CUSTOMER_ID]);

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error('Error updating customer:', error);
    return NextResponse.json(
      { error: 'Failed to update customer' },
      { status: 500 }
    );
  }
}
