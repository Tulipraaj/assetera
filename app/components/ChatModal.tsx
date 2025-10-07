'use client';

import { useEffect, useRef, useState } from 'react';
import { X, Send } from 'lucide-react';
import { Message } from '@/lib/types/chat';
import { supabase } from '@/lib/supabaseClient';
import { useUser } from '@/lib/hooks/useUser';

interface ChatModalProps {
  isOpen: boolean;
  onClose: () => void;
  customer: {
    CUSTOMER_ID: number;
    CONTACT_FIRST_NAME: string;
    CONTACT_LAST_NAME: string;
  };
}

export default function ChatModal({ isOpen, onClose, customer }: ChatModalProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [newMessage, setNewMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const { user } = useUser();

  useEffect(() => {
    if (!isOpen || !user) return;

    const markMessagesAsRead = async () => {
      try {
        const { error } = await supabase
          .from('messages')
          .update({ read: true })
          .eq('customer_id', customer.CUSTOMER_ID)
          .eq('sender', 'customer')
          .eq('read', false);

        if (error) {
          console.error('Error marking messages as read:', error);
        }
      } catch (error) {
        console.error('Error marking messages as read:', error);
      }
    };

    // Fetch existing messages when the modal opens
    const fetchMessages = async () => {
      try {
        // First check if we're authenticated
        const { data: authData, error: authError } = await supabase.auth.getSession();
        if (authError) {
          console.error('Auth error:', authError);
          return;
        }

        // Mark messages as read when opening the chat
        await markMessagesAsRead();
        if (!authData.session) {
          console.error('No active session');
          return;
        }
        console.log('Current session:', authData);

        // Verify advisor authorization
        const { data: advisor, error: advisorError } = await supabase
          .from('advisors')
          .select('id')
          .eq('id', authData.session.user.id)
          .single();

        if (advisorError || !advisor) {
          console.error('Not authorized as advisor:', advisorError);
          return;
        }

        const { data, error } = await supabase
          .from('messages')
          .select(`
            id,
            customer_id,
            advisor_id,
            sender,
            content,
            created_at,
            read
          `)
          .eq('customer_id', customer.CUSTOMER_ID)
          .order('created_at', { ascending: true });

        if (error) {
          console.error('Error fetching messages:', error);
          throw error;
        } else {
          console.log('Fetched messages:', data);
          setMessages(data || []);
        }
      } catch (error) {
        console.error('Detailed error:', error);
      }
    };

    fetchMessages();

    // Subscribe to new messages
    const channel = supabase
      .channel(`chat:${customer.CUSTOMER_ID}`)
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'messages',
          filter: `customer_id=eq.${customer.CUSTOMER_ID}`
        },
        (payload: any) => {
          const newMessage = payload.new as Message;
          setMessages(prev => [...prev, newMessage]);
        }
      )
      .subscribe();

    // Also subscribe to updates to existing messages (for read status)
    const updateChannel = supabase
      .channel(`chat-updates:${customer.CUSTOMER_ID}`)
      .on(
        'postgres_changes',
        {
          event: 'UPDATE',
          schema: 'public',
          table: 'messages',
          filter: `customer_id=eq.${customer.CUSTOMER_ID}`
        },
        (payload: any) => {
          const updatedMessage = payload.new as Message;
          setMessages(prev => 
            prev.map(msg => 
              msg.id === updatedMessage.id ? updatedMessage : msg
            )
          );
        }
      )
      .subscribe();

    return () => {
      channel.unsubscribe();
      updateChannel.unsubscribe();
    };
  }, [isOpen, customer.CUSTOMER_ID]);

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newMessage.trim() || !user) return;

    setLoading(true);
    try {
      // First verify our authentication status
      const { data: authData, error: authError } = await supabase.auth.getSession();
      if (authError) {
        console.error('Auth error:', authError);
        throw authError;
      }
      console.log('Current session before sending:', authData);

      if (!authData.session?.user) {
        throw new Error('No authenticated user found');
      }

      // Verify advisor authorization
      const { data: advisor, error: advisorError } = await supabase
        .from('advisors')
        .select('id')
        .eq('id', authData.session.user.id)
        .single();

      if (advisorError || !advisor) {
        console.error('Not authorized as advisor:', advisorError);
        throw new Error('Not authorized as advisor');
      }

      const message = {
        customer_id: customer.CUSTOMER_ID,
        advisor_id: authData.session.user.id,
        sender: 'advisor',
        content: newMessage.trim(),
        created_at: new Date().toISOString(),
        read: false
      };

      console.log('Attempting to send message:', message);

      const { data, error } = await supabase
        .from('messages')
        .insert([message])
        .select()
        .single();

      if (error) {
        console.error('Database error:', error);
        throw error;
      }

      console.log('Message sent successfully:', data);
      // Update the messages state immediately with the new message
      setMessages(prev => [...prev, data]);
      setNewMessage('');
    } catch (error) {
      console.error('Detailed error in handleSendMessage:', error);
      alert('Failed to send message. Please check console for details.');
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg w-full max-w-lg mx-4">
        {/* Header */}
        <div className="flex justify-between items-center p-4 border-b">
          <h3 className="text-lg font-semibold">
            Chat with {customer.CONTACT_FIRST_NAME} {customer.CONTACT_LAST_NAME}
          </h3>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700"
          >
            <X size={24} />
          </button>
        </div>

        {/* Messages */}
        <div className="h-[400px] overflow-y-auto p-4">
          {messages.map((message) => (
            <div
              key={message.id}
              className={`mb-4 flex ${
                message.sender === 'advisor' ? 'justify-end' : 'justify-start'
              }`}
            >
              <div
                className={`rounded-lg px-4 py-2 max-w-[70%] break-words ${
                  message.sender === 'advisor'
                    ? 'bg-indigo-600 text-white'
                    : 'bg-gray-100 text-gray-900'
                }`}
              >
                <p>{message.content}</p>
                <div className="flex justify-between items-center mt-1">
                  <span className="text-xs opacity-75">
                    {new Date(message.created_at).toLocaleTimeString()}
                  </span>
                  {message.sender === 'customer' && (
                    <span className={`text-xs ml-2 ${message.read ? 'text-green-600' : 'text-gray-400'}`}>
                      {message.read ? '✓✓' : '✓'}
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>

        {/* Message Input */}
        <form onSubmit={handleSendMessage} className="border-t p-4">
          <div className="flex gap-2">
            <input
              type="text"
              value={newMessage}
              onChange={(e) => setNewMessage(e.target.value)}
              placeholder="Type a message..."
              className="flex-1 p-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500"
              disabled={loading}
            />
            <button
              type="submit"
              disabled={loading || !newMessage.trim()}
              className="bg-indigo-600 text-white px-4 py-2 rounded-md hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Send size={20} />
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
