const UUIDLength = 32;
const RangeMax = Math.pow(16, UUIDLength);

export const uuidRoot = (uuid: string, n: number = UUIDLength): string => {
  if (n < 1 || n > UUIDLength) {
    throw new Error(`UUID root must have a length between 1 and ${UUIDLength}`);
  }

  const dashes = n > 20 ? 4 : (n > 16 ? 3 : (n > 12 ? 2 : (n > 8 ? 1 : 0)));

  return uuid.slice(0, n + dashes);
};

export const uuidRootForHashRange = (
  uuid: string,
  range: number = RangeMax,
): string => {
  if (range < 1 || range > RangeMax) {
    throw new Error(
      `UUID root range must have a length between 1 and ${RangeMax}`,
    );
  }

  for (let i = 1; i <= 32; i++) {
    if (Math.pow(16, i) >= range) {
      return uuidRoot(uuid, i);
    }
  }

  throw new Error(); // never
};
