import { CustomerWithFunds } from '@/lib/types';
import { RISK_LEVELS, FUND_NAMES } from '@/lib/constants';
import { MessageCircle } from 'lucide-react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import { useState } from 'react';
import { useRouter } from 'next/navigation';
import ChatModal from '@/app/components/ChatModal';
import NotificationBadge from '@/app/components/NotificationBadge';

interface CustomerCardProps {
  customer: CustomerWithFunds;
}

export default function CustomerCard({ customer }: CustomerCardProps) {
  const router = useRouter();
  const [isChatOpen, setIsChatOpen] = useState(false);
  
  // Use values from FUND_PREDICTIONS_FINAL if available
  const risk = customer.FUND_PREDICTIONS_FINAL?.FINAL_RISK ?? customer.FINAL_RISK;
  const fundId = customer.FUND_PREDICTIONS_FINAL?.FUND_ID ?? customer.FUND_ID;

  // 5 distinct color pairs for risk levels 1-5
  const riskColors: Record<number, { bg: string; color: string }> = {
    1: { bg: '#E0F2FE', color: '#0369A1' }, // blue
    2: { bg: '#DCFCE7', color: '#166534' }, // green
    3: { bg: '#FEF9C3', color: '#92400E' }, // yellow
    4: { bg: '#FDE68A', color: '#B45309' }, // orange
    5: { bg: '#FECACA', color: '#B91C1C' }, // red
  };
  const riskColor = riskColors[risk] || { bg: '#E5E7EB', color: '#374151' };

  return (
    <>
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="bg-white rounded-xl shadow-lg p-6 hover:shadow-xl transition-all duration-300 group"
      >
        <div 
          className="cursor-pointer"
          onClick={() => router.push(`/customer/${customer.CUSTOMER_ID}`)}
        >
          <div className="flex justify-between items-start mb-4">
            <div>
              <h3 className="text-lg font-semibold text-gray-900 group-hover:text-blue-600 transition-colors">
                {customer.CONTACT_FIRST_NAME} {customer.CONTACT_LAST_NAME}
              </h3>
              <p className="text-sm text-gray-500">ID : {customer.CUSTOMER_ID}</p>
            </div>
            <div
              className="px-3 py-1 rounded-full text-sm font-medium transform group-hover:scale-105 transition-all"
              style={{
                backgroundColor: riskColor.bg,
                color: riskColor.color,
              }}
            >
              {RISK_LEVELS[risk as keyof typeof RISK_LEVELS]}
            </div>
          </div>
          
          <div className="space-y-2 mb-4">
            <div className="flex justify-between items-center py-1">
              <span className="text-sm text-gray-500">Age</span>
              <span className="text-sm text-gray-900">{customer.AGE}</span>
            </div>
            <div className="flex justify-between items-center py-1">
              <span className="text-sm text-gray-500">Gender</span>
              <span className="text-sm text-gray-900">{customer.GENDER}</span>
            </div>
            <div className="flex justify-between items-center py-1">
              <span className="text-sm text-gray-500">Marital Status</span>
              <span className="text-sm text-gray-900">{customer.MARITAL_STATUS}</span>
            </div>
            <div className="flex justify-between items-center py-1">
              <span className="text-sm text-gray-500">Fund</span>
              <span className="text-sm text-gray-900 font-medium">
                {fundId ? FUND_NAMES[fundId as keyof typeof FUND_NAMES] : 'Not Assigned'}
              </span>
            </div>
            <div className="flex justify-between items-center py-1">
              <span className="text-sm text-gray-500">Total Assets</span>
              <span className="text-sm text-gray-900 font-medium">
                ${customer.TOTAL_ASSETS?.toLocaleString() || '0'}
              </span>
            </div>
          </div>
        </div>
        
        <div className="pt-2 border-t border-gray-100">
          <button
            onClick={(e) => {
              e.stopPropagation();
              setIsChatOpen(true);
            }}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-gradient-to-r from-indigo-500 to-blue-600 text-white rounded-md shadow-sm text-sm font-medium hover:from-indigo-600 hover:to-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 relative transform hover:translate-y-[-1px] transition-all"
          >
            <MessageCircle size={16} />
            Chat with Customer
            <NotificationBadge 
              customerId={customer.CUSTOMER_ID} 
              onClick={() => setIsChatOpen(true)}
            />
          </button>
        </div>
      </motion.div>
      
      <ChatModal
        isOpen={isChatOpen}
        onClose={() => setIsChatOpen(false)}
        customer={customer}
      />
    </>
  );
}
