'use client';

import { useState, use as usePromise } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Customer, CustomerUpdatePayload } from '@/lib/types';
import { motion } from 'framer-motion';
import { useRouter } from 'next/navigation';
import ProfileCard from '@/app/components/ProfileCard';


export default function CustomerProfile({ params }: { params: Promise<{ id: string }> }) {
  const { id } = usePromise(params);
  const router = useRouter();
  const [isEditing, setIsEditing] = useState(false);
  const [editedCustomer, setEditedCustomer] = useState<{
    FINAL_RISK?: 'Very Low' | 'Low' | 'Average' | 'High' | 'Very High';
    FUND_ID?: string;
  }>({});

  const { data: customer, isLoading, error } = useQuery({
    queryKey: ['customer', id],
    queryFn: async () => {
      const response = await fetch(`/api/customers/${id}`);
      if (!response.ok) {
        throw new Error('Failed to fetch customer');
      }
      return response.json();
    }
  });

  const mutation = useMutation({
    mutationFn: async (updates: CustomerUpdatePayload) => {
      const response = await fetch('/api/customers/update', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(updates),
      });
      if (!response.ok) {
        throw new Error('Failed to update customer');
      }
      return response.json();
    },
    onSuccess: () => {
      setIsEditing(false);
      router.refresh();
    },
  });

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-3xl mx-auto">
          <div className="animate-pulse space-y-4">
            <div className="h-8 bg-white rounded w-1/4" />
            <div className="bg-white rounded-lg shadow-md p-6 space-y-4">
              <div className="h-4 bg-gray-200 rounded w-1/2" />
              <div className="h-4 bg-gray-200 rounded w-3/4" />
              <div className="h-4 bg-gray-200 rounded w-2/3" />
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !customer) {
    return (
      <div className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-3xl mx-auto">
          <div className="bg-red-50 p-4 rounded-md">
            <h3 className="text-sm font-medium text-red-800">
              Error loading customer profile
            </h3>
          </div>
        </div>
      </div>
    );
  }

  // Use values from FUND_PREDICTIONS_FINAL if available
  const risk = customer.FUND_PREDICTIONS_FINAL?.FINAL_RISK ?? customer.FINAL_RISK;
  const fundId = customer.FUND_PREDICTIONS_FINAL?.FUND_ID ?? customer.FUND_ID;

  // Fund ID to Name mapping
  const fundIdNameMap: Record<string | number, string> = {
    1: 'Shield Fund',
    2: 'Core Fund',
    3: 'Pro Growth',
    4: 'Vertex 26  Fund',
    5: 'Alpha 31 Fund',
  };
  // Risk number to name mapping
  const riskNameMap: Record<string | number, string> = {
    1: 'Very Low',
    2: 'Low',
    3: 'Average',
    4: 'High',
    5: 'Very High',
  };
  // Risk color mapping (same as home page)
  const riskColors: Record<number | string, { bg: string; color: string }> = {
    1: { bg: '#E0F2FE', color: '#0369A1' }, // blue
    2: { bg: '#DCFCE7', color: '#166534' }, // green
    3: { bg: '#FEF9C3', color: '#92400E' }, // yellow
    4: { bg: '#FDE68A', color: '#B45309' }, // orange
    5: { bg: '#FECACA', color: '#B91C1C' }, // red
  };
  const riskColor = riskColors[risk] || { bg: '#E5E7EB', color: '#374151' };

  const handleEdit = () => {
    setEditedCustomer({
      FINAL_RISK: risk,
      FUND_ID: fundId,
    });
    setIsEditing(true);
  };

  const handleSave = () => {
    mutation.mutate({
      CUSTOMER_ID: customer.CUSTOMER_ID,
      FINAL_RISK: Number(editedCustomer.FINAL_RISK ?? customer.FINAL_RISK),
      FUND_ID: Number(editedCustomer.FUND_ID ?? customer.FUND_ID),
    });
  };

  const handleCancel = () => {
    setIsEditing(false);
    setEditedCustomer({});
  };

  return (
    <motion.main
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-7xl mx-auto">
        <h2 className="text-2xl font-bold mb-8 text-gray-900">Customer Profile</h2>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Left column - Personal Information */}
          <div className="md:col-span-2">
            <ProfileCard
              title="Personal Information"
              colorClass="border-l-4 border-blue-500"
            >
              <dl className="grid grid-cols-1 gap-4">
                <div className="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Customer ID</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{customer.CUSTOMER_ID}</dd>
                </div>
                <div className="bg-white px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Full Name</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                    {customer.CONTACT_FIRST_NAME} {customer.CONTACT_LAST_NAME}
                  </dd>
                </div>
                <div className="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Address</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                    {customer.STREET}<br />
                    {customer.CITY}, {customer.STATE} {customer.ZIP}<br />
                    {customer.COUNTRY}
                  </dd>
                </div>
                <div className="bg-white px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Age</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{customer.AGE}</dd>
                </div>
                <div className="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Marital Status</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{customer.MARITAL_STATUS}</dd>
                </div>
                <div className="bg-white px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Gender</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{customer.GENDER}</dd>
                </div>
                <div className="bg-gray-50 px-4 py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                  <dt className="text-sm font-medium text-gray-500">Dependents</dt>
                  <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{customer.NUMBER_OF_DEPENDENTS}</dd>
                </div>
              </dl>
            </ProfileCard>
          </div>
          
          {/* Right column - Investment Information */}
          <div className="md:col-span-1 space-y-6">
            {/* Risk Profile Card */}
            <ProfileCard
              title="Risk Profile"
              colorClass="border-l-4 border-red-500"
            >
              <div className="p-6">
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-medium text-gray-900">Current Risk Level</h3>
                  {!isEditing && (
                    <button
                      onClick={handleEdit}
                      className="p-2 text-gray-400 hover:text-blue-500 transition-colors"
                    >
                      <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                      </svg>
                    </button>
                  )}
                </div>
                {isEditing ? (
                  <select
                    value={editedCustomer.FINAL_RISK || risk}
                    onChange={(e) => setEditedCustomer({
                      ...editedCustomer,
                      FINAL_RISK: e.target.value as 'Very Low' | 'Low' | 'Average' | 'High' | 'Very High'
                    })}
                    className="w-full p-3 text-lg border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring focus:ring-blue-200 transition-all"
                  >
                    <option value="Very Low">Very Low</option>
                    <option value="Low">Low</option>
                    <option value="Average">Average</option>
                    <option value="High">High</option>
                    <option value="Very High">Very High</option>
                  </select>
                ) : (
                  <div
                    className="text-center p-4 rounded-lg text-lg font-medium"
                    style={{ backgroundColor: riskColor.bg, color: riskColor.color }}
                  >
                    {riskNameMap[risk] || risk}
                  </div>
                )}
              </div>
            </ProfileCard>

            {/* Fund Allocation Card */}
            <ProfileCard
              title="Fund Allocation"
              colorClass="border-l-4 border-purple-500"
            >
              <div className="p-6">
                <div className="flex justify-between items-center mb-4">
                  <h3 className="text-lg font-medium text-gray-900">Current Fund</h3>
                  {!isEditing && (
                    <button
                      onClick={handleEdit}
                      className="p-2 text-gray-400 hover:text-blue-500 transition-colors"
                    >
                      <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                      </svg>
                    </button>
                  )}
                </div>
                {isEditing ? (
                  <select
                    value={editedCustomer.FUND_ID || fundId}
                    onChange={e => setEditedCustomer({
                      ...editedCustomer,
                      FUND_ID: e.target.value
                    })}
                    className="w-full p-3 text-lg border-2 border-gray-300 rounded-lg focus:border-blue-500 focus:ring focus:ring-blue-200 transition-all"
                  >
                    <option value="">Select Fund</option>
                    {Object.entries(fundIdNameMap).map(([id, name]) => (
                      <option key={id} value={id}>{`Fund ${id} - ${name}`}</option>
                    ))}
                  </select>
                ) : (
                  <div className="text-center p-4 bg-purple-50 text-purple-700 rounded-lg text-lg font-medium">
                    {fundId ? `${fundIdNameMap[fundId] || ''}` : 'Not Assigned'}
                  </div>
                )}
              </div>
            </ProfileCard>

            {/* Edit buttons */}
            {isEditing && (
              <div className="flex justify-end space-x-4 mt-6">
                <button
                  type="button"
                  onClick={handleCancel}
                  className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-all"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleSave}
                  className="px-4 py-2 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-md hover:from-blue-700 hover:to-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-all"
                >
                  Save Changes
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </motion.main>
  );
}
