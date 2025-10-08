import { NextRequest, NextResponse } from "next/server";

export async function GET(
  request: NextRequest,
  { params }: { params: { slug: string } },
) {
  const { slug } = params;
  return NextResponse.json({ message: `Hello ${slug}!` });
}
