import { filesDir } from "../src/constants";
import puppeteer from "puppeteer";

//https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView&page__ind=&v_dep_cod=01&v_pro_cod=01&v_dis_cod=01

const trueSource = 'https://prod3.seace.gob.pe/portalseace-uiwd-pub/ControllerServletOpen?pageid=0&portletid=BuscadorUbigeo&scriptdo=doView';

const browser = await puppeteer.launch();
const page = await browser.newPage();
await page.goto(trueSource);

type District = {
	code: string;
	value: string;
};

type Province = {
	code: string;
	value: string;
	districts: District[];
};

type Region = {
	code: string;
	value: string;
	provinces: Province[];
};

const exteriorRegion: Region = {
	code: "98",
	value: "EXTERIOR",
	provinces: [
		{
			code: "01",
			value: "EXTERIOR",
			districts: [
				{
					code: "00",
					value: "MULTIDISTRITAL"
				}
			]
		}
	]
}

const regionsWithoutProvinces = await page.evaluate(() => {
	const $regionsSelector = document.querySelector(`select[name="v_dep_cod"]`)!;
	const $regionOptions = $regionsSelector.querySelectorAll("option")!;

	const regionsWithoutProvinces = Array
		.from($regionOptions)
		.map($region => {
			const regionCode = $region.getAttribute("value")!;

			if (regionCode === "00" || regionCode === "98") {
				return null;
			}

			const regionValue = $region.textContent?.replace(`${regionCode}-`, "").trim()!;

			const region: Omit<Region, "provinces"> = {
				code: regionCode,
				value: regionValue
			}

			return region;
		})
		.filter(Boolean) as Omit<Region, "provinces">[];

	return regionsWithoutProvinces;
});

const regionsWithoutDistricts: (Omit<Region, "provinces"> & {
	provinces: Omit<Province, "districts">[];
})[] = []

for await (const regionWithoutProvinces of regionsWithoutProvinces) {
	const regionPage = await browser.newPage();
	const regionUrl = `${trueSource}&v_dep_cod=${regionWithoutProvinces.code}`;

	console.log(`Searching provinces for region ${regionWithoutProvinces.value}`);
	await regionPage.goto(regionUrl);

	const provincesWithOutDistricts: Omit<Province, "districts">[] = await regionPage.evaluate(() => {
		const $provincesSelector = document.querySelector(`select[name="v_pro_cod"]`);
		if (!$provincesSelector) {
			throw new Error("Provinces selector not found");
		}

		const $provinceOptions = $provincesSelector.querySelectorAll("option");
		if (!$provinceOptions) {
			throw new Error("Provinces options not found");
		}


		const provinces = Array
			.from($provinceOptions)
			.map($province => {
				const provinceCode = $province.getAttribute("value")!;

				if (provinceCode === "00") {
					return null;
				}

				const provinceValue = $province.textContent?.replace(`${provinceCode}-`, "").trim()!;

				const province: Omit<Province, "districts"> = {
					code: provinceCode,
					value: provinceValue
				}

				return province;
			})
			.filter(Boolean) as Omit<Province, "districts">[];

		return provinces;
	});

	regionsWithoutDistricts.push({
		...regionWithoutProvinces,
		provinces: provincesWithOutDistricts
	});
}

const regions: Region[] = []

for await (const regionWithoutDistricts of regionsWithoutDistricts) {
	const regionBaseUrl = `${trueSource}&v_dep_cod=${regionWithoutDistricts.code}`;

	const districtPage = await browser.newPage();

	for await (const provinceWithoutDistrict of regionWithoutDistricts.provinces) {
		const provinceUrl = `${regionBaseUrl}&v_pro_cod=${provinceWithoutDistrict.code}`;

		console.log(`Searching districts for region ${regionWithoutDistricts.value} for province ${provinceWithoutDistrict.value}`);
		await districtPage.goto(provinceUrl);

		const districts: District[] = await districtPage.evaluate(() => {
			const $districtsSelector = document.querySelector(`select[name="v_dis_cod"]`);
			if (!$districtsSelector) {
				throw new Error("Districts selector not found");
			}

			const $districtOptions = $districtsSelector.querySelectorAll("option");
			if (!$districtOptions) {
				throw new Error("Districts options not found");
			}

			const districts = Array
				.from($districtOptions)
				.map($district => {
					const districtCode = $district.getAttribute("value")!;

					if (districtCode === "00") {
						return null;
					}

					const districtValue = $district.textContent?.replace(`${districtCode}-`, "").trim()!;

					const district: District = {
						code: districtCode,
						value: districtValue
					}

					return district;
				})
				.filter(Boolean) as District[];

			return districts;
		});

		regions.push({
			code: regionWithoutDistricts.code,
			value: regionWithoutDistricts.value,
			provinces: [
				{
					code: provinceWithoutDistrict.code,
					value: provinceWithoutDistrict.value,
					districts: districts
				}
			]
		});
	}
}

regions.push(exteriorRegion);

const jsonFile = `${filesDir}/ubigeo.json`;
Bun.write(jsonFile, JSON.stringify(regions, null, 2));

const copyFile = `${filesDir}/ubigeo.copy.sql`;
const copyFileHeader = `COPY public."Ubigeo" (codigo, region, provincia, distrito) FROM stdin;\n`;
const copyFileContent = regions.reduce((acc, region) => {
	const regionContent = region.provinces.reduce((acc, province) => {
		const provinceContent = province.districts.reduce((acc, district) => {
			return `${acc}${region.code}${province.code}${district.code}\t${region.value}\t${province.value}\t${district.value}\n`;
		}, "");

		return `${acc}${provinceContent}`;
	}, "");

	return `${acc}${regionContent}`;
}, "");

Bun.write(copyFile, `${copyFileHeader}${copyFileContent}`);

await browser.close();