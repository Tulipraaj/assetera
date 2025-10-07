export interface Message {
  id: string;
  customer_id: number;
  advisor_id: string;
  sender: 'advisor' | 'customer';
  content: string;
  created_at: string;
  read: boolean;
}

export interface ChatParticipant {
  id: string | number;
  type: 'advisor' | 'customer';
  name: string;
}
