use crate::core::config::secret::generate_secret_key;
use anyhow::{Context, Result};
use std::{fs, path::Path};

pub fn create_gitignore(project_dir: &Path) -> Result<()> {
    let gitignore_content = ".secret.key\nstorage/\ndatabase.db\n.data/\nsample_data/\n";

    fs::write(project_dir.join(".gitignore"), gitignore_content)
        .context("Failed to write .gitignore")?;

    Ok(())
}

pub fn create_secret_key(project_dir: &Path) -> Result<()> {
    let key_path = project_dir.join(".secret.key");

    if key_path.exists() {
        return Ok(());
    }

    let key = generate_secret_key().context("Failed to generate secret key")?;

    fs::write(&key_path, &key).context("Failed to write secret key file")?;

    set_secret_key_permissions(&key_path).context("Failed to set secret key file permissions")?;

    Ok(())
}

fn set_secret_key_permissions(path: &Path) -> Result<()> {
    let perms = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::Permissions::from_mode(0o600)
        }
        #[cfg(not(unix))]
        {
            let mut perms = fs::metadata(path)?.permissions();
            perms.set_readonly(true);
            perms
        }
    };

    fs::set_permissions(path, perms)?;
    Ok(())
}
