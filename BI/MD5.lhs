> module BI.MD5 (md5) where

> import Char
> import Bits
> import Word

> type ABCD = (Word32, Word32, Word32, Word32)
> type XYZ = (Word32, Word32, Word32)
> type Rotation = Int

MD5 test suite:
MD5 ("") = d41d8cd98f00b204e9800998ecf8427e
MD5 ("a") = 0cc175b9c0f1b6a831c399e269772661
MD5 ("abc") = 900150983cd24fb0d6963f7d28e17f72
MD5 ("message digest") = f96b697d7cb7938d525a2f31aaf161d0
MD5 ("abcdefghijklmnopqrstuvwxyz") = c3fcd3d76192e4007dfb496cca67e13b
MD5 ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789") =
d174ab98d277d9f5a5611c2c9f419d9f
MD5 ("123456789012345678901234567890123456789012345678901234567890123456
78901234567890") = 57edf4a22be3c955ac49da2e2107b67a

md5test :: IO()
md5test = foldr (\x y -> putStr (md5 x) >> putStr "\n" >> y) (putStr "") ["", "a", "abc", "message digest", "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "12345678901234567890123456789012345678901234567890123456789012345678901234567890"]

MD5> md5test
d41d8cd98f00b204e9800998ecf8427e
0cc175b9c0f1b6a831c399e269772661
900150983cd24fb0d6963f7d28e17f72
f96b697d7cb7938d525a2f31aaf161d0
c3fcd3d76192e4007dfb496cca67e13b
d174ab98d277d9f5a5611c2c9f419d9f
57edf4a22be3c955ac49da2e2107b67a

MD5> 

> md5 :: String -> String
> md5 s = s5
>  where s1_2 = md5_step_1_2_pad_length s
>        abcd = md5_step_3_init
>        abcd' = md5_step_4_main abcd s1_2
>        s5 = md5_step_5_display abcd'

> md5_step_1_2_pad_length :: String -> String
> md5_step_1_2_pad_length s = md5_step_1_2_work 0 s

> md5_step_1_2_work :: Integer -> String -> String
> md5_step_1_2_work c64 "" = padding ++ len
>  where padding = '\128':replicate (fromInteger (((448 - (c64 + 1)) `mod` 512) `div` 8)) '\000'
>        len = map chr $ size_split 8 (fromInteger c64)
> md5_step_1_2_work c64 (c:cs) = c:md5_step_1_2_work c64' cs
>  where c64' = (c64 + 8) `mod` (2^64)

> size_split :: Int -> Word32 -> [Int]
> size_split 0 _ = []
> size_split p n = (fromIntegral d):size_split (p-1) n'
>  where (n', d) = divMod n 256

> md5_step_3_init :: ABCD
> md5_step_3_init = (0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476)

> md5_step_4_main :: ABCD -> String -> ABCD
> md5_step_4_main abcd "" = abcd
> md5_step_4_main (a, b, c, d) s = md5_step_4_main abcd4 s'
>  where (s64, s') = takeDrop 64 s
>        (r1, r2, r3, r4) = rounds
>        abcd0 = (a, b, c, d)
>        abcd1 = md5_step_4_round md5_step_4_f abcd0 s16_0 r1
>        abcd2 = md5_step_4_round md5_step_4_g abcd1 s16_1 r2
>        abcd3 = md5_step_4_round md5_step_4_h abcd2 s16_2 r3
>        (a', b', c', d') = md5_step_4_round md5_step_4_i abcd3 s16_3 r4
>        abcd4 = (a + a', b + b', c + c', d + d')
>        s16_0 = get_word_32s s64
>        s16_1 = map (\x -> s16_0 !! x) [(5 * x + 1) `mod` 16 | x <- [0..15]]
>        s16_2 = map (\x -> s16_0 !! x) [(3 * x + 5) `mod` 16 | x <- [0..15]]
>        s16_3 = map (\x -> s16_0 !! x) [(7 * x) `mod` 16 | x <- [0..15]]

> get_word_32s :: String -> [Word32]
> get_word_32s "" = []
> get_word_32s ss = this:rest
>  where (s, ss') = takeDrop 4 ss
>        this = sum $ zipWith (*) (map (fromIntegral.ord) s)
>                                 [256^x | x <- [0..3] :: [Int]]
>        rest = get_word_32s ss'

> md5_step_4_round :: (XYZ -> Word32) -> ABCD -> [Word32]
>                                     -> [(Rotation, Word32)] -> ABCD
> md5_step_4_round f (a, b, c, d) s ns = foldl (doit f) (a, b, c, d) ns'
>  where ns' = zipWith (\x (y, z) -> (x,y,z)) s ns

> doit :: (XYZ -> Word32) -> ABCD -> (Word32, Rotation, Word32) -> ABCD
> doit f (a, b, c, d) (k, s, i) = (d, a', b, c)
>  where mid_a = a + f(b,c,d) + k + i
>        rot_a = rotL mid_a s
>        a' = b + rot_a

> md5_step_4_f :: XYZ -> Word32
> md5_step_4_f (x, y, z) = (x .&. y) .|. ((complement x) .&. z)

> md5_step_4_g :: XYZ -> Word32
> md5_step_4_g (x, y, z) = (x .&. z) .|. (y .&. (complement z))

> md5_step_4_h :: XYZ -> Word32
> md5_step_4_h (x, y, z) = x `xor` y `xor` z

> md5_step_4_i :: XYZ -> Word32
> md5_step_4_i (x, y, z) = y `xor` (x .|. (complement z))

> rounds :: ([(Rotation, Word32)],
>            [(Rotation, Word32)],
>            [(Rotation, Word32)],
>            [(Rotation, Word32)])
> rounds = (r1, r2, r3, r4)
>  where r1 = [(s11, 0xd76aa478), (s12, 0xe8c7b756), (s13, 0x242070db),
>              (s14, 0xc1bdceee), (s11, 0xf57c0faf), (s12, 0x4787c62a),
>              (s13, 0xa8304613), (s14, 0xfd469501), (s11, 0x698098d8),
>              (s12, 0x8b44f7af), (s13, 0xffff5bb1), (s14, 0x895cd7be),
>              (s11, 0x6b901122), (s12, 0xfd987193), (s13, 0xa679438e),
>              (s14, 0x49b40821)]
>        r2 = [(s21, 0xf61e2562), (s22, 0xc040b340), (s23, 0x265e5a51),
>              (s24, 0xe9b6c7aa), (s21, 0xd62f105d), (s22,  0x2441453),
>              (s23, 0xd8a1e681), (s24, 0xe7d3fbc8), (s21, 0x21e1cde6),
>              (s22, 0xc33707d6), (s23, 0xf4d50d87), (s24, 0x455a14ed),
>              (s21, 0xa9e3e905), (s22, 0xfcefa3f8), (s23, 0x676f02d9),
>              (s24, 0x8d2a4c8a)]
>        r3 = [(s31, 0xfffa3942), (s32, 0x8771f681), (s33, 0x6d9d6122),
>              (s34, 0xfde5380c), (s31, 0xa4beea44), (s32, 0x4bdecfa9),
>              (s33, 0xf6bb4b60), (s34, 0xbebfbc70), (s31, 0x289b7ec6),
>              (s32, 0xeaa127fa), (s33, 0xd4ef3085), (s34,  0x4881d05),
>              (s31, 0xd9d4d039), (s32, 0xe6db99e5), (s33, 0x1fa27cf8),
>              (s34, 0xc4ac5665)]
>        r4 = [(s41, 0xf4292244), (s42, 0x432aff97), (s43, 0xab9423a7),
>              (s44, 0xfc93a039), (s41, 0x655b59c3), (s42, 0x8f0ccc92),
>              (s43, 0xffeff47d), (s44, 0x85845dd1), (s41, 0x6fa87e4f),
>              (s42, 0xfe2ce6e0), (s43, 0xa3014314), (s44, 0x4e0811a1),
>              (s41, 0xf7537e82), (s42, 0xbd3af235), (s43, 0x2ad7d2bb),
>              (s44, 0xeb86d391)]
>        s11 = 7
>        s12 = 12
>        s13 = 17
>        s14 = 22
>        s21 = 5
>        s22 = 9
>        s23 = 14
>        s24 = 20
>        s31 = 4
>        s32 = 11
>        s33 = 16
>        s34 = 23
>        s41 = 6
>        s42 = 10
>        s43 = 15
>        s44 = 21

> takeDrop :: Int -> [a] -> ([a], [a])
> takeDrop _ [] = ([], [])
> takeDrop 0 xs = ([], xs)
> takeDrop n (x:xs) = (x:ys, zs)
>  where (ys, zs) = takeDrop (n-1) xs

> md5_step_5_display :: ABCD -> String
> md5_step_5_display (a,b,c,d) = concat $ map display_32bits_as_hex [a,b,c,d]

> display_32bits_as_hex :: Word32 -> String
> display_32bits_as_hex x0 = map getc [y2,y1,y4,y3,y6,y5,y8,y7]
>  where (x1, y1) = divMod x0 16
>        (x2, y2) = divMod x1 16
>        (x3, y3) = divMod x2 16
>        (x4, y4) = divMod x3 16
>        (x5, y5) = divMod x4 16
>        (x6, y6) = divMod x5 16
>        (y8, y7) = divMod x6 16
>        getc n = (['0'..'9'] ++ ['a'..'f']) !! (fromIntegral n)

> rotL :: Word32 -> Rotation -> Word32
> rotL a s = shiftL a s .|. shiftL a (s-32)

