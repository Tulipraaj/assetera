'use client';

import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Customer, CustomerUpdatePayload } from '@/lib/types';
import { motion } from 'framer-motion';
import { useRouter } from 'next/navigation';
import ProfileCard from '@/app/components/ProfileCard';

export default function CustomerProfile({ params }: { params: { id: string } }) {
  const router = useRouter();
  const [isEditing, setIsEditing] = useState(false);
  const [editedCustomer, setEditedCustomer] = useState<{
    FINAL_RISK?: string;
    FUND_ID?: string;
  }>({});

  const { data: customer, isLoading, error } = useQuery({
    queryKey: ['customer', params.id],
    queryFn: async () => {
      const response = await fetch(`/api/customers/${params.id}`);
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

  // Fund ID to Name mapping
  const fundIdNameMap: Record<string | number, string> = {
    1: 'Core Income',
    2: 'Pro Core',
    3: 'Pro Growth',
    4: 'Redeem Surge 31',
    5: 'Bridge Growth 26',
  };
  
  // Risk number to name mapping
  const riskNameMap: Record<string | number, string> = {
    1: 'Very Low',
    2: 'Low',
    3: 'Average',
    4: 'High',
    5: 'Very High',
  };
  
  // Risk color mapping
  const riskColors: Record<number | string, { bg: string; color: string }> = {
    1: { bg: '#E0F2FE', color: '#0369A1' }, // blue
    2: { bg: '#DCFCE7', color: '#166534' }, // green
    3: { bg: '#FEF9C3', color: '#92400E' }, // yellow
    4: { bg: '#FDE68A', color: '#B45309' }, // orange
    5: { bg: '#FECACA', color: '#B91C1C' }, // red
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="animate-pulse space-y-6">
            <div className="h-8 bg-gray-200 rounded w-1/4" />
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="md:col-span-2 bg-white rounded-lg h-96" />
              <div className="space-y-6">
                <div className="bg-white rounded-lg h-32" />
                <div className="bg-white rounded-lg h-32" />
                <div className="bg-white rounded-lg h-32" />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !customer) {
    return (
      <div className="min-h-screen bg-gray-100 py-8 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
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
  const riskColor = riskColors[risk] || { bg: '#E5E7EB', color: '#374151' };

  const handleEdit = () => {
    setEditedCustomer({
      FINAL_RISK: String(risk),
      FUND_ID: String(fundId),
    });
    setIsEditing(true);
  };

  const handleSave = () => {
    mutation.mutate({
      CUSTOMER_ID: customer.CUSTOMER_ID,
      FINAL_RISK: Number(editedCustomer.FINAL_RISK || risk),
      FUND_ID: Number(editedCustomer.FUND_ID || fundId),
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
                <div className="grid grid-cols-3 items-center">
                  <dt className="text-sm font-medium text-gray-500">Full Name</dt>
                  <dd className="text-sm text-gray-900 col-span-2">
                    {customer.CONTACT_FIRST_NAME} {customer.CONTACT_LAST_NAME}
                  </dd>
                </div>
                <div className="grid grid-cols-3 items-center">
                  <dt className="text-sm font-medium text-gray-500">Age</dt>
                  <dd className="text-sm text-gray-900 col-span-2">{customer.AGE}</dd>
                </div>
                <div className="grid grid-cols-3 items-center">
                  <dt className="text-sm font-medium text-gray-500">Gender</dt>
                  <dd className="text-sm text-gray-900 col-span-2">{customer.GENDER}</dd>
                </div>
                <div className="grid grid-cols-3 items-center">
                  <dt className="text-sm font-medium text-gray-500">Marital Status</dt>
                  <dd className="text-sm text-gray-900 col-span-2">{customer.MARITAL_STATUS}</dd>
                </div>
                <div className="grid grid-cols-3 items-center">
                  <dt className="text-sm font-medium text-gray-500">Address</dt>
                  <dd className="text-sm text-gray-900 col-span-2">
                    {customer.STREET}<br />
                    {customer.CITY}, {customer.STATE} {customer.ZIP}<br />
                    {customer.COUNTRY}
                  </dd>
                </div>
              </dl>
            </ProfileCard>
          </div>

          {/* Right column - Risk, Fund, and Assets */}
          <div className="space-y-6">
            <ProfileCard
              title="Risk Profile"
              onEdit={handleEdit}
              colorClass="border-l-4 border-red-500"
            >
              {isEditing ? (
                <select
                  value={editedCustomer.FINAL_RISK || risk}
                  onChange={(e) => setEditedCustomer({
                    ...editedCustomer,
                    FINAL_RISK: e.target.value
                  })}
                  className="w-full p-2 border rounded"
                >
                  {Object.entries(riskNameMap).map(([value, name]) => (
                    <option key={value} value={value}>{name}</option>
                  ))}
                </select>
              ) : (
                <div
                  className="inline-block px-3 py-1 rounded-full text-sm font-medium"
                  style={{ backgroundColor: riskColor.bg, color: riskColor.color }}
                >
                  {riskNameMap[risk] || risk}
                </div>
              )}
            </ProfileCard>

            <ProfileCard
              title="Investment Fund"
              onEdit={handleEdit}
              colorClass="border-l-4 border-green-500"
            >
              {isEditing ? (
                <select
                  value={editedCustomer.FUND_ID || fundId}
                  onChange={(e) => setEditedCustomer({
                    ...editedCustomer,
                    FUND_ID: e.target.value
                  })}
                  className="w-full p-2 border rounded"
                >
                  {Object.entries(fundIdNameMap).map(([id, name]) => (
                    <option key={id} value={id}>Fund {id} - {name}</option>
                  ))}
                </select>
              ) : (
                <div className="text-gray-900">
                  {fundId ? `Fund ${fundId} - ${fundIdNameMap[fundId]}` : 'Not Assigned'}
                </div>
              )}
            </ProfileCard>

            <ProfileCard
              title="Assets Overview"
              colorClass="border-l-4 border-purple-500"
            >
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Total Assets</span>
                  <span className="text-gray-900 font-medium">
                    ${customer.TOTAL_ASSETS?.toLocaleString() || '0'}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Dependents</span>
                  <span className="text-gray-900">{customer.NUMBER_OF_DEPENDENTS}</span>
                </div>
              </div>
            </ProfileCard>

            {isEditing && (
              <div className="flex gap-4 mt-4">
                <button
                  onClick={handleCancel}
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSave}
                  className="flex-1 px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
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
