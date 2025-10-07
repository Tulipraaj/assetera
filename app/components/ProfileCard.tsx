import { Pencil } from 'lucide-react';

interface CustomerCardProps {
  title: string;
  children: React.ReactNode;
  onEdit?: () => void;
  colorClass: string;
}

export default function ProfileCard({ title, children, onEdit, colorClass }: CustomerCardProps) {
  return (
    <div className={`bg-white rounded-lg shadow-md overflow-hidden ${colorClass}`}>
      <div className="px-4 py-5 sm:px-6 flex justify-between items-center">
        <h3 className="text-lg leading-6 font-medium text-gray-900">
          {title}
        </h3>
        {onEdit && (
          <button
            onClick={onEdit}
            className="p-2 rounded-full hover:bg-gray-100 transition-colors"
          >
            <Pencil size={16} className="text-gray-600" />
          </button>
        )}
      </div>
      <div className="border-t border-gray-200 px-4 py-5 sm:p-6">
        {children}
      </div>
    </div>
  );
}
