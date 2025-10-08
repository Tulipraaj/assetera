'use client';

import { useEffect, useState, useMemo } from 'react';
import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';
import { useQuery } from '@tanstack/react-query';
import { CustomerWithFunds } from '@/lib/types';
import SearchBar from '@/app/components/SearchBar';
import CustomerCard from '@/app/components/CustomerCard';
import Pagination from '@/app/components/Pagination';

const LoginPage = dynamic(() => import('../login/page'), {
  ssr: false
});

export default function Home() {
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);
  const [riskFilter, setRiskFilter] = useState<string>('');
  const [fundIdFilter, setFundIdFilter] = useState<string>('');
  const [pageSize, setPageSize] = useState<number>(100);

  interface CustomersResponse {
    customers: CustomerWithFunds[];
    total: number;
    totalPages: number;
  }

  const { data, isLoading, error } = useQuery<CustomersResponse>({
    queryKey: ['customers', search, page, riskFilter, fundIdFilter, pageSize],
    queryFn: async () => {
      const params = new URLSearchParams({
        page: String(page),
        limit: String(pageSize),
        search: search,
      });
      if (riskFilter) params.append('risk', riskFilter);
      if (fundIdFilter) params.append('fundId', fundIdFilter);
      const response = await fetch(`/api/customers?${params.toString()}`);
      if (!response.ok) {
        throw new Error('Failed to fetch customers');
      }
      const result = await response.json();
      return {
        customers: result.customers,
        total: result.total,
        totalPages: Math.ceil(result.total / pageSize)
      };
    },
    placeholderData: (previousData) => previousData,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000
  });

  const handleSearch = (value: string) => {
    setSearch(value);
    setPage(1);
  };

  // Get unique risk and fundId values for dropdowns, always include 1-5
  const riskOptions = useMemo(() => {
    return ['1', '2', '3', '4', '5'];
  }, []);

  const fundIdOptions = useMemo(() => {
    return ['1', '2', '3', '4', '5'];
  }, []);

  const handleRiskChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setRiskFilter(e.target.value);
    setPage(1);
  };
  const handleFundIdChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setFundIdFilter(e.target.value);
    setPage(1);
  };
  const handlePageSizeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setPageSize(Number(e.target.value));
    setPage(1);
  };

  // Risk and Fund mappings
  const riskNameMap: Record<string, string> = {
    '1': 'Very Low',
    '2': 'Low',
    '3': 'Average',
    '4': 'High',
    '5': 'Very High',
  };
  const fundIdNameMap: Record<string, string> = {
    '1': 'Shield Fund',
    '2': 'Core Fund',
    '3': 'Pro Growth',
    '4': 'Vertex 26  Fund',
    '5': 'Alpha 31 Fund',
  };

  if (isLoading) {
    return (
      <main className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="md:flex md:items-center md:justify-between mb-8">
            <div className="flex-1 min-w-0">
              <div className="animate-pulse h-8 bg-gray-200 rounded w-64" />
            </div>
            <div className="mt-4 md:mt-0 md:ml-4">
              <div className="animate-pulse h-10 bg-gray-200 rounded w-48" />
            </div>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
            {Array.from({ length: 9 }).map((_, i) => (
              <div key={i} className="animate-pulse bg-white rounded-lg shadow-md p-6">
                <div className="flex justify-between items-start mb-4">
                  <div className="space-y-2">
                    <div className="h-5 bg-gray-200 rounded w-32" />
                    <div className="h-4 bg-gray-200 rounded w-24" />
                  </div>
                  <div className="h-6 bg-gray-200 rounded w-20" />
                </div>
                <div className="space-y-3 mb-4">
                  {Array.from({ length: 5 }).map((_, j) => (
                    <div key={j} className="flex justify-between">
                      <div className="h-4 bg-gray-200 rounded w-20" />
                      <div className="h-4 bg-gray-200 rounded w-24" />
                    </div>
                  ))}
                </div>
                <div className="h-9 bg-gray-200 rounded w-full" />
              </div>
            ))}
          </div>
        </div>
      </main>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto text-center">
          <div className="bg-red-50 p-4 rounded-md">
            <h3 className="text-sm font-medium text-red-800">
              Error loading customers
            </h3>
          </div>
        </div>
      </div>
    );
  }

  const { customers, total, totalPages } = data || { customers: [] as CustomerWithFunds[], total: 0, totalPages: 0 };

  return (
    <main className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
      <div className="max-w-7xl mx-auto">
        <div className="md:flex md:items-center md:justify-between mb-8">
          <div className="flex-1 min-w-0">
            <h2 className="text-2xl font-bold leading-7 text-gray-900 sm:text-3xl sm:truncate">
              Financial Advisor Dashboard
            </h2>
          </div>
          <div className="mt-4 md:mt-0 md:ml-4 flex flex-col md:flex-row gap-2">
            <SearchBar value={search} onChange={handleSearch} />
            <select
              className="border rounded px-2 py-1 text-sm"
              value={riskFilter}
              onChange={handleRiskChange}
            >
              <option value="">All Risks</option>
              {riskOptions.map(risk => (
                <option key={risk} value={risk}>{`Risk ${risk} - ${riskNameMap[risk]}`}</option>
              ))}
            </select>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={fundIdFilter}
              onChange={handleFundIdChange}
            >
              <option value="">All Funds</option>
              {fundIdOptions.map(fundId => (
                <option key={fundId} value={fundId}>{`Fund ${fundId} - ${fundIdNameMap[fundId]}`}</option>
              ))}
            </select>
            <select
              className="border rounded px-2 py-1 text-sm"
              value={pageSize}
              onChange={handlePageSizeChange}
            >
              {[10, 25, 50, 100, 250, 500, 1000].map(size => (
                <option key={size} value={size}>{size} per page</option>
              ))}
            </select>
          </div>
        </div>

        <motion.div 
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3"
        >
          {customers.map((customer: CustomerWithFunds) => (
            <CustomerCard key={customer.CUSTOMER_ID} customer={customer} />
          ))}
        </motion.div>

        {totalPages > 1 && (
          <Pagination
            currentPage={page}
            totalPages={totalPages}
            onPageChange={setPage}
          />
        )}

        
      </div>
    </main>
  );
}
