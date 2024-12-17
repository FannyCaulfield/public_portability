'use client';

interface ConsentModalProps {
  isOpen: boolean;
  onAccept: () => void;
  onDecline: () => void;
}

export default function ConsentModal({ isOpen, onAccept, onDecline }: ConsentModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-8 max-w-md mx-4 relative">
        <div className="flex items-center gap-3 mb-4">
          <svg 
            className="w-8 h-8 text-blue-500"
            fill="none" 
            strokeWidth="1.5" 
            stroke="currentColor" 
            viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
          </svg>
          <h3 className="text-xl font-semibold text-white">
            Confirmation d'upload
          </h3>
        </div>

        <div className="mb-6">
          <p className="text-gray-300 mb-4">
            En uploadant vos fichiers Twitter, vous confirmez que :
          </p>
          <ul className="text-gray-300 list-disc list-inside space-y-2">
            <li>Vous êtes le propriétaire légitime de ces données.</li>
            <li>Vous acceptez que les donnees fournies soient utilisées par le Centre Nationale de la Recherche Scientifique (CNRS) dans le cadre de l'opération HelloQuitteX.</li>
            <li>Vous comprenez que ces données seront utilisées pour créer votre profil sur HelloQuitteX.</li>
          </ul>
        </div>

        <div className="flex gap-4 justify-end">
          <button
            onClick={onDecline}
            className="px-4 py-2 text-sm font-medium text-gray-300 hover:text-white transition-colors"
          >
            Annuler
          </button>
          <button
            onClick={onAccept}
            className="px-4 py-2 text-sm font-medium bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Accepter et continuer
          </button>
        </div>
      </div>
    </div>
  );
}