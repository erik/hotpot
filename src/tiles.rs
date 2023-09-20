#[derive(Copy, Clone)]
pub struct Tile {
    x: u32,
    y: u32,
    z: u8,
}

impl Tile {
    pub fn new(z: u8, x: u32, y: u32) -> Self {
        Self { z, x, y }
    }

    pub fn parent(&self) -> Self {
        if self.z == 0 {
            *self
        } else {
            Tile::new(self.z - 1, self.x / 2, self.y / 2)
        }
    }
}
