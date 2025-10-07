'use client';

import { useEffect, useState } from 'react';
import { supabase } from '@/lib/supabaseClient';
interface NotificationBadgeProps {
  customerId: number;
  onClick?: () => void;
}

export default function NotificationBadge({ customerId, onClick }: NotificationBadgeProps) {
  const [unreadCount, setUnreadCount] = useState(0);

  useEffect(() => {
    const fetchUnreadCount = async () => {
      const { data, error } = await supabase
        .from('messages')
        .select('id', { count: 'exact' })
        .eq('customer_id', customerId)
        .eq('sender', 'customer')
        .eq('read', false);

      if (!error && data) {
        setUnreadCount(data.length);
      }
    };

    // Initial fetch
    fetchUnreadCount();

    // Subscribe to new messages
    const channel = supabase
      .channel(`chat:${customerId}`)
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'messages',
          filter: `customer_id=eq.${customerId} AND sender=eq.customer AND read=eq.false`
        },
        (payload) => {
          setUnreadCount(prev => prev + 1);
        }
      )
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'messages',
          filter: `customer_id=eq.${customerId} AND sender=eq.customer`
        },
        () => {
          fetchUnreadCount(); // Refetch count on updates
        }
      )
      .subscribe();

    return () => {
      channel.unsubscribe();
    };
  }, [customerId]);

  if (unreadCount === 0) return null;

  return (
    <div
      onClick={onClick}
      className="absolute -top-2 -right-2 bg-red-500 text-white text-xs rounded-full h-5 w-5 flex items-center justify-center cursor-pointer"
    >
      {unreadCount}
    </div>
  );
}
