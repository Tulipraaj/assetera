'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import CustomerCard from '@/app/components/CustomerCard';
import { CustomerWithFunds } from '@/lib/types';
import { RISK_LEVELS, FUND_NAMES } from '@/lib/constants';

export default function FinancialAdvisorPage() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [risk, setRisk] = useState('');
  const [fundId, setFundId] = useState('');
  const [limit, setLimit] = useState('10');

  const { data, isLoading, error } = useQuery({
    queryKey: ['customers', page, search, risk, fundId, limit],
    queryFn: async () => {
      const params = new URLSearchParams({
        page: page.toString(),
        limit,
        search,
        risk,
        fundId,
      });

      const response = await fetch(`/api/customers?${params}`);
      if (!response.ok) {
        throw new Error('Failed to fetch customers');
      }
      return response.json();
    }
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin h-8 w-8 border-4 border-primary border-t-transparent rounded-full"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-red-500">Error loading customers. Please try again later.</div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-4">Customer Dashboard</h1>
        
        {/* Search and Filter Controls */}
        <div className="flex flex-wrap gap-4 mb-6">
          <input
            type="text"
            placeholder="Search customers..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setPage(1); // Reset to first page on new search
            }}
            className="flex-1 min-w-[200px] p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500"
          />
          
          <select
            value={risk}
            onChange={(e) => {
              setRisk(e.target.value);
              setPage(1);
            }}
            className="p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            <option value="">All Risk Levels</option>
            {Object.entries(RISK_LEVELS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>

          <select
            value={fundId}
            onChange={(e) => {
              setFundId(e.target.value);
              setPage(1);
            }}
            className="p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            <option value="">All Funds</option>
            {Object.entries(FUND_NAMES).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>

          <select
            value={limit}
            onChange={(e) => {
              setLimit(e.target.value);
              setPage(1); // Reset to first page when changing limit
            }}
            className="p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            <option value="5">5 per page</option>
            <option value="10">10 per page</option>
            <option value="20">20 per page</option>
            <option value="50">50 per page</option>
          </select>
        </div>
      </div>

      {/* Customer Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {data?.customers.map((customer: CustomerWithFunds) => (
          <CustomerCard key={customer.CUSTOMER_ID} customer={customer} />
        ))}
      </div>

      {/* Pagination */}
      {data?.totalPages > 1 && (
        <div className="flex justify-center items-center space-x-4 mt-8">
          <button
            onClick={() => setPage(prev => Math.max(prev - 1, 1))}
            disabled={page === 1}
            className="px-4 py-2 border border-gray-300 rounded-md disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
          >
            Previous
          </button>
          <span className="text-gray-700">
            Page {page} of {data.totalPages}
          </span>
          <button
            onClick={() => setPage(prev => Math.min(prev + 1, data.totalPages))}
            disabled={page === data.totalPages}
            className="px-4 py-2 border border-gray-300 rounded-md disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
