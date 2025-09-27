use std::{collections::HashMap, io::Result};

use crate::{
    buffer::{Buffer, Serialize},
    netdata::bdb::{BdbPermission, permission_target::BdbPermissionTarget},
};

#[derive(Debug, Clone, Default)]
pub struct BdbFilePermissions {
    pub default: HashMap<BdbPermissionTarget, BdbPermission>,
    pub current: HashMap<BdbPermissionTarget, BdbPermission>,
}

impl Serialize for BdbFilePermissions {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write(
            &self
                .default
                .iter()
                .map(|(target, perms)| (*target, perms.bits()))
                .collect::<HashMap<_, _>>(),
        )?;
        buffer.write(
            &self
                .current
                .iter()
                .map(|(target, perms)| (*target, perms.bits()))
                .collect::<HashMap<_, _>>(),
        )?;

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            default: buffer
                .read::<HashMap<_, u8>>()?
                .into_iter()
                .map(|(target, perms)| (target, BdbPermission::from_bits_retain(perms)))
                .collect::<HashMap<_, _>>(),
            current: buffer
                .read::<HashMap<_, u8>>()?
                .into_iter()
                .map(|(target, perms)| (target, BdbPermission::from_bits_retain(perms)))
                .collect::<HashMap<_, _>>(),
        })
    }
}
