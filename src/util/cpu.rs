use core_affinity::CoreId;

#[derive(Copy, Clone, Debug)]
pub struct CpuAffinity {
    id: Option<CoreId>,
}

impl CpuAffinity {
    #[inline]
    pub fn for_cores() -> Vec<CpuAffinity> {
        if let Some(cpus) = core_affinity::get_core_ids() {
            // Only supported on Linux, Windows and MacOs
            let mut v1 = Vec::default();
            let mut v2 = Vec::default();
            for (i, cpu) in cpus.into_iter().enumerate() {
                let cpu_affinity = CpuAffinity { id: Some(cpu) };
                if i % 2 == 0 {
                    v1.push(cpu_affinity);
                } else {
                    v2.push(cpu_affinity);
                }
            }
            v2.reverse();
            v1.extend(v2);
            v1
        } else {
            Vec::default()
        }
    }

    #[inline]
    pub fn none() -> Self {
        Self { id: None }
    }

    #[inline]
    pub fn apply_for_current(self) {
        if let Some(id) = self.id {
            core_affinity::set_for_current(id);
        }
    }
}

impl Default for CpuAffinity {
    #[inline]
    fn default() -> Self {
        Self::none()
    }
}
