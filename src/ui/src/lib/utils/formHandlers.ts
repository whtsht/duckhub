import { get } from 'svelte/store';
import { t } from '../i18n';

export interface FormSubmitResult {
  success: boolean;
  data?: any;
  error?: string;
}

export interface FormHandlerConfig {
  entityType: string;
  mode: 'create' | 'edit';
  entityName?: string;
}

export function showFormSuccessToast(config: FormHandlerConfig) {
  const entityType = config.entityType;
  const mode = config.mode;
  const entityName = config.entityName || '';

  let messageKey: string;
  if (mode === 'create') {
    messageKey = `${entityType}.create_success`;
  } else {
    messageKey = `${entityType}.edit.success`;
  }

  const message = get(t)(messageKey, { values: { name: entityName } });
  window.showToast?.success(message);
}

export function showFormErrorToast(error: string, entityType: string) {
  const message = get(t)(`${entityType}.error.general`, { values: { error } });
  window.showToast?.error(message);
}

export function showDeleteSuccessToast(entityType: string, entityName: string) {
  const messageKey = `${entityType}.delete.success`;
  const message = get(t)(messageKey, { values: { name: entityName } });
  window.showToast?.success(message);
}
