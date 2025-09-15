export interface ShowPanelProps<T = any> {
  entity: T | null;
  title: string;
  loading?: boolean;
  error?: string | null;
  onEdit?: () => void;
  onDelete?: () => void;
  customActions?: ActionButton[];
}

export interface FormPanelProps<T = any> {
  mode: 'create' | 'edit';
  title: string;
  initialData?: T | null;
  loading?: boolean;
  error?: string | null;
  onSave?: (data: T) => void;
  onCancel?: () => void;
}

export interface ActionButton {
  label: string;
  icon?: any;
  onClick: () => void;
  variant?: 'primary' | 'secondary' | 'danger';
  disabled?: boolean;
}
